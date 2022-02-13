#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "free_list.h"
#include "key.h"
#include "llsm/pg_options.h"
#include "llsm/slice.h"
#include "llsm/status.h"
#include "persist/segment_file.h"
#include "segment_info.h"
#include "tlx/btree_map.h"
#include "util/thread_pool.h"
#include "workspace.h"

namespace llsm {
namespace pg {

class Manager {
 public:
  static Manager LoadIntoNew(const std::filesystem::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const PageGroupedDBOptions& options);

  static Manager Reopen(const std::filesystem::path& db,
                        const PageGroupedDBOptions& options);

  Status Get(const Key& key, std::string* value_out);

  // Pre-condition: The batch is sorted in ascending order by key.
  Status PutBatch(const std::vector<std::pair<Key, Slice>>& records);

  // Will read partial segments.
  Status ScanWithEstimates(
      const Key& start_key, const size_t amount,
      std::vector<std::pair<Key, std::string>>* values_out);

  // Only reads whole segments.
  Status ScanWhole(const Key& start_key, const size_t amount,
                   std::vector<std::pair<Key, std::string>>* values_out);

  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out) {
    return ScanWithEstimates(start_key, amount, values_out);
  }

  // Benchmark statistics.
  const std::vector<size_t>& GetReadCounts() const { return w_.read_counts(); }
  const std::vector<size_t>& GetWriteCounts() const {
    return w_.write_counts();
  }

  Manager(const Manager&) = delete;
  Manager& operator=(const Manager&) = delete;

  Manager(Manager&&) = default;
  Manager& operator=(Manager&&) = default;

  // Not intended for external use (used by the tests).
  auto IndexBeginIterator() const { return index_.begin(); }
  auto IndexEndIterator() const { return index_.end(); }
  size_t NumSegmentFiles() const { return segment_files_.size(); }

 private:
  Manager(std::filesystem::path db_path,
          std::vector<std::pair<Key, SegmentInfo>> boundaries,
          std::vector<SegmentFile> segment_files,
          PageGroupedDBOptions options, uint32_t next_sequence_number,
          FreeList free);

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

  // Write the range [start_idx, end_idx) into the given segment.
  Status WriteToSegment(Key segment_base, SegmentInfo* sinfo,
                        const std::vector<std::pair<Key, Slice>>& records,
                        size_t start_idx, size_t end_idx);

  // Rewrite the segment specified by `segment_base` (merge in the overflows)
  // while also adding in additional records.
  //
  // If `consider_adjacent` is true, this method will also rewrite all logically
  // neighboring segments that also have overflows.
  void RewriteSegments(Key segment_base,
                       std::vector<Record>::const_iterator addtl_rec_begin,
                       std::vector<Record>::const_iterator addtl_rec_end);

  // Flatten the given page chain and merge in the additional records (which
  // must fall in the key space assigned to the given page chain).
  void FlattenChain(Key base,
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

  auto SegmentForKey(Key key) {
    auto it = index_.upper_bound(key);
    if (it != index_.begin()) {
      --it;
    }
    return it;
  }

  auto SegmentForKey(Key key) const {
    auto it = index_.upper_bound(key);
    if (it != index_.begin()) {
      --it;
    }
    return it;
  }

  std::filesystem::path db_path_;
  tlx::btree_map<Key, SegmentInfo> index_;
  std::vector<SegmentFile> segment_files_;
  uint32_t next_sequence_number_;
  FreeList free_;
  std::unique_ptr<ThreadPool> bg_threads_;

  // Options passed in when the `Manager` was created.
  PageGroupedDBOptions options_;

  // Holds state used by individual worker threads.
  // This is static for convenience (to use `thread_local`). So for correctness
  // there can only be one active `Manager` in a process at any time.
  static thread_local Workspace w_;

  static const std::string kSegmentFilePrefix;
};

}  // namespace pg
}  // namespace llsm
