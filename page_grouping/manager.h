#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

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
  Status PutBatch(const std::vector<std::pair<Key, Slice>>& records);
  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out);

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
          Options options);

  static Manager BulkLoadIntoSegments(
      const std::filesystem::path& db_path,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::Options& options);

  static Manager BulkLoadIntoPages(
      const std::filesystem::path& db,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::Options& options);

  std::filesystem::path db_path_;
  tlx::btree_map<Key, SegmentInfo> index_;
  std::vector<SegmentFile> segment_files_;

  // Options passed in when the `Manager` was created.
  Options options_;

  // Holds state used by individual worker threads.
  // This is static for convenience (to use `thread_local`). So for correctness
  // there can only be one active `Manager` in a process at any time.
  static thread_local Workspace w_;
};

}  // namespace pg
}  // namespace llsm
