#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "free_list.h"
#include "key.h"
#include "llsm/slice.h"
#include "llsm/status.h"
#include "persist/segment_file.h"
#include "segment_info.h"
#include "tlx/btree_map.h"
#include "workspace.h"

namespace llsm {
namespace pg {

class Manager {
 public:
  struct Options {
    // If set to false, no segments larger than 1 page will be created.
    bool use_segments = true;

    // By default, put 45 +/- (2 * 5) records into each page.
    size_t records_per_page_goal = 45;
    size_t records_per_page_delta = 5;

    // If set to true, will write out the segment sizes and models to a CSV file
    // for debug purposes.
    bool write_debug_info = true;

    // If set to false, direct I/O will be disabled (used for end-to-end tests).
    bool use_direct_io = true;
  };
  static Manager LoadIntoNew(const std::filesystem::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const Options& options);

  static Manager Reopen(const std::filesystem::path& db,
                        const Options& options);

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
          std::vector<SegmentFile> segment_files, Options options,
          uint32_t next_sequence_number, FreeList free);

  static Manager BulkLoadIntoSegments(
      const std::filesystem::path& db_path,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::Options& options);

  static Manager BulkLoadIntoPages(
      const std::filesystem::path& db,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::Options& options);

  // Write the range [start_idx, end_idx) into the given segment.
  Status WriteToSegment(Key segment_base, const SegmentInfo& sinfo,
                        const std::vector<std::pair<Key, Slice>>& records,
                        size_t start_idx, size_t end_idx);

  // Helpers for convenience.
  void ReadPage(const SegmentId& seg_id, size_t page_idx, void* buffer);
  void WritePage(const SegmentId& seg_id, size_t page_idx, void* buffer);

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

  // Options passed in when the `Manager` was created.
  Options options_;

  // Holds state used by individual worker threads.
  // This is static for convenience (to use `thread_local`). So for correctness
  // there can only be one active `Manager` in a process at any time.
  static thread_local Workspace w_;

  static const std::string kSegmentFilePrefix;
};

}  // namespace pg
}  // namespace llsm
