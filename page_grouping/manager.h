#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "llsm/slice.h"
#include "llsm/status.h"
#include "tlx/btree_map.h"

namespace llsm {
namespace pg {

using Key = uint64_t;

class Manager {
 public:
  struct LoadOptions {
    // If set to false, no segments larger than 1 page will be created.
    bool use_segments = true;
    // By default, put 50 +/- 10 records into each page.
    size_t records_per_page_goal = 50;
    size_t records_per_page_delta = 10;
  };
  static Manager LoadIntoNew(
      std::filesystem::path db,
      const std::vector<std::pair<Key, Slice>>& records,
      const LoadOptions& options);

  static Manager Reopen(std::filesystem::path db);

  Status Get(const Key& key, std::string* value_out);
  Status PutBatch(const std::vector<std::pair<Key, Slice>>& records);
  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out);

 private:
  tlx::btree_map<Key, uint64_t> index_;
};

}  // namespace pg
}  // namespace llsm
