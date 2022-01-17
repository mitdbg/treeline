#include "manager.h"

namespace llsm {
namespace pg {

Manager Manager::LoadIntoNew(
    std::filesystem::path db,
    const std::vector<std::pair<Key, const Slice>>& records) {
  return Manager();
}

Manager Manager::Reopen(std::filesystem::path db) { return Manager(); }

Status Manager::Get(const Key& key, std::string* value_out) {
  return Status::OK();
}

Status Manager::PutBatch(
    const std::vector<std::pair<Key, const Slice>>& records) {
  return Status::OK();
}

Status Manager::Scan(const Key& start_key, const size_t amount,
                     std::vector<std::pair<Key, std::string>>* values_out) {
  return Status::OK();
}

}  // namespace pg
}  // namespace llsm
