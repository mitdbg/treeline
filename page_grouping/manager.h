#pragma once

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "free_list.h"
#include "key.h"
#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "treeline/status.h"
#include "lock_manager.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "segment_index.h"
#include "segment_info.h"
#include "util/insert_tracker.h"
#include "util/thread_pool.h"
#include "workspace.h"

namespace tl {
namespace pg {

// This implementation currently only supports unsigned integer keys up to 64
// bits. `kMinReservedKey` and `kMaxReservedKey` are reserved keys; they should
// not be used.
class Manager {
 public:
  static constexpr Key kMinReservedKey = 0;
  static constexpr Key kMaxReservedKey = std::numeric_limits<Key>::max();

  static Manager LoadIntoNew(const std::filesystem::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const PageGroupedDBOptions& options);

  static Manager Reopen(const std::filesystem::path& db,
                        const PageGroupedDBOptions& options);

  void SetTracker(std::shared_ptr<InsertTracker> tracker);

  Status Get(const Key& key, std::string* value_out);

  // Similar to `Get()`, but also returns the page(s) read from disk (e.g., for
  // access to other records for caching purposes).
  //
  // Callers should not store the returned `Page`s because their backing memory
  // is only valid until the next call to a `Manager` method.
  std::pair<Status, std::vector<pg::Page>> GetWithPages(const Key& key,
                                                        std::string* value_out);

  // Pre-condition: The batch is sorted in ascending order by key.
  Status PutBatch(const std::vector<std::pair<Key, Slice>>& records);

  // Does the same thing as `PutBatch()` but attempts to parallelize the write.
  // Pre-condition: The batch is sorted in ascending order by key.
  Status PutBatchParallel(const std::vector<std::pair<Key, Slice>>& records);

  // Will read partial segments.
  Status ScanWithEstimates(
      const Key& start_key, const size_t amount,
      std::vector<std::pair<Key, std::string>>* values_out);

  // Only reads whole segments.
  Status ScanWhole(const Key& start_key, const size_t amount,
                   std::vector<std::pair<Key, std::string>>* values_out);

  // Cannot run concurrently with writes (this implementation is experimental).
  Status ScanWithExperimentalPrefetching(
      const Key& start_key, const size_t amount,
      std::vector<std::pair<Key, std::string>>* values_out);

  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out) {
    return ScanWithEstimates(start_key, amount, values_out);
  }

  // Returns the boundaries of the page on which `key` should be stored.
  // The lower bound is inclusive and the upper bound is exclusive.
  std::pair<Key, Key> GetPageBoundsFor(const Key key) const;

  // See `PageGroupedDB::FlattenRange()` for details.
  Status FlattenRange(const Key start_key = 0,
                      const Key end_key = std::numeric_limits<Key>::max());

  // Benchmark statistics.
  const std::vector<size_t>& GetReadCounts() const { return w_.read_counts(); }
  const std::vector<size_t>& GetWriteCounts() const {
    return w_.write_counts();
  }
  void PostStats() const;

  Manager(const Manager&) = delete;
  Manager& operator=(const Manager&) = delete;

  Manager(Manager&&) = default;
  Manager& operator=(Manager&&) = default;

  // Not intended for external use (used by the tests).
  auto IndexBeginIterator() const { return index_->BeginIterator(); }
  auto IndexEndIterator() const { return index_->EndIterator(); }
  size_t NumSegmentFiles() const { return segment_files_.size(); }

 private:
  Manager(std::filesystem::path db_path,
          std::vector<std::pair<Key, SegmentInfo>> boundaries,
          std::vector<std::unique_ptr<SegmentFile>> segment_files,
          PageGroupedDBOptions options, uint32_t next_sequence_number,
          std::unique_ptr<FreeList> free);

  static Manager BulkLoadIntoSegments(
      const std::filesystem::path& db_path,
      const std::vector<std::pair<Key, Slice>>& records,
      const PageGroupedDBOptions& options);
  void BulkLoadIntoSegmentsImpl(const std::vector<Record>& records);

  static Manager BulkLoadIntoPages(
      const std::filesystem::path& db,
      const std::vector<std::pair<Key, Slice>>& records,
      const PageGroupedDBOptions& options);
  void BulkLoadIntoPagesImpl(const std::vector<Record>& records);

  Status PutBatchImpl(const std::vector<std::pair<Key, Slice>>& records,
                      size_t start_idx, size_t end_idx);

  // Write the range [start_idx, end_idx) into the given segment. The caller
  // must already hold a `kPageWrite` lock on the segment. This method will
  // release the segment lock when it is done making the write(s).
  //
  // This method returns the number of records actually written. This number may
  // be less than the number of records passed to the method; this indicates
  // that a reorganization intervened during the write. If this happens, the
  // caller should retry the write.
  size_t WriteToSegment(const SegmentIndex::Entry& segment,
                        const std::vector<std::pair<Key, Slice>>& records,
                        size_t start_idx, size_t end_idx);

  // Rewrite the segment specified by `segment_base` (merge in the overflows)
  // while also adding in additional records.
  //
  // If `consider_adjacent` is true, this method will also rewrite all logically
  // neighboring segments that also have overflows.
  //
  // This method may return a non-OK status which indicates that the additional
  // records passed in do not belong to the specified segment and that the
  // rewrite was aborted. This happens when a concurrent reorg intervenes.
  Status RewriteSegments(Key segment_base,
                         std::vector<Record>::const_iterator addtl_rec_begin,
                         std::vector<Record>::const_iterator addtl_rec_end);
  // NOTE: `segments_to_rewrite` must consist of contiguous segments and the
  // caller must already hold the appropriate locks.
  Status RewriteSegmentsImpl(
      std::vector<SegmentIndex::Entry> segments_to_rewrite,
      std::vector<Record>::const_iterator addtl_rec_begin,
      std::vector<Record>::const_iterator addtl_rec_end);

  // Flatten the given page chain and merge in the additional records (which
  // must fall in the key space assigned to the given page chain).
  //
  // This method may return a non-OK status which indicates that the additional
  // records passed in do not belong to the specified segment and that the
  // flatten was aborted. This happens when a concurrent reorg intervenes.
  Status FlattenChain(Key base,
                      std::vector<Record>::const_iterator addtl_rec_begin,
                      std::vector<Record>::const_iterator addtl_rec_end);

  // Helpers for convenience.
  void ReadPage(const SegmentId& seg_id, size_t page_idx, void* buffer) const;
  void WritePage(const SegmentId& seg_id, size_t page_idx, void* buffer) const;
  // Reads the given segment into this thread's workspace buffer.
  void ReadSegment(const SegmentId& seg_id) const;
  void ReadOverflows(
      const std::vector<std::pair<SegmentId, void*>>& overflows_to_read) const;

  std::pair<Key, SegmentInfo> LoadIntoNewSegment(uint32_t sequence_number,
                                                 const Segment& segment,
                                                 Key upper_bound);

  // Loads the records in `[rec_begin, rec_end)` into pages based on the page
  // fill goal.
  std::vector<std::pair<Key, SegmentInfo>> LoadIntoNewPages(
      uint32_t sequence_number, Key lower_bound, Key upper_bound,
      std::vector<Record>::const_iterator rec_begin,
      std::vector<Record>::const_iterator rec_end);

  std::filesystem::path db_path_;
  std::shared_ptr<LockManager> lock_manager_;
  std::unique_ptr<SegmentIndex> index_;
  std::vector<std::unique_ptr<SegmentFile>> segment_files_;
  uint32_t next_sequence_number_;
  std::unique_ptr<FreeList> free_;
  std::unique_ptr<ThreadPool> bg_threads_;
  std::shared_ptr<InsertTracker> tracker_;

  // Options passed in when the `Manager` was created.
  PageGroupedDBOptions options_;

  // Holds state used by individual worker threads.
  // This is static for convenience (to use `thread_local`). So for correctness
  // there can only be one active `Manager` in a process at any time.
  static thread_local Workspace w_;

  static const std::string kSegmentFilePrefix;
};

}  // namespace pg
}  // namespace tl
