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
  static Manager LoadIntoNew(
      std::filesystem::path db,
      const std::vector<std::pair<Key, const Slice>>& records);

  static Manager Reopen(std::filesystem::path db);

  Status Get(const Key& key, std::string* value_out);
  Status PutBatch(const std::vector<std::pair<Key, const Slice>>& records);
  Status Scan(const Key& start_key, const size_t amount,
              std::vector<std::pair<Key, std::string>>* values_out);

 private:
  tlx::btree_map<Key, uint64_t> index_;
};

}  // namespace pg
}  // namespace llsm
