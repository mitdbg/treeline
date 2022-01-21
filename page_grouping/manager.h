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

namespace llsm {
namespace pg {

class Manager {
 public:
  struct LoadOptions {
    // If set to false, no segments larger than 1 page will be created.
    bool use_segments = true;
    // By default, put 45 +/- (2 * 5) records into each page.
    size_t records_per_page_goal = 45;
    size_t records_per_page_delta = 5;
    // If set to true, will write out the segment sizes and models to a CSV file
    // for debug purposes.
    bool write_debug_info = true;
  };
  static Manager LoadIntoNew(const std::filesystem::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const LoadOptions& options);

  static Manager Reopen(const std::filesystem::path& db);

  Status Get(const Key& key, std::string* value_out);
  Status PutBatch(const std::vector<std::pair<Key, Slice>>& records);
  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out);

  Manager(const Manager&) = delete;
  Manager& operator=(const Manager&) = delete;

  Manager(Manager&&) = default;
  Manager& operator=(Manager&&) = default;

 private:
  Manager(std::filesystem::path db_path,
          std::vector<std::pair<Key, SegmentInfo>> boundaries,
          std::vector<SegmentFile> segment_files);

  static Manager BulkLoadIntoSegments(
      const std::filesystem::path& db_path,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::LoadOptions& options);

  static Manager BulkLoadIntoPages(
      const std::filesystem::path& db,
      const std::vector<std::pair<Key, Slice>>& records,
      const Manager::LoadOptions& options);

  std::filesystem::path db_path_;
  tlx::btree_map<Key, SegmentInfo> index_;
  std::vector<SegmentFile> segment_files_;
};

}  // namespace pg
}  // namespace llsm
