#include "pg_db_impl.h"

namespace llsm {
namespace pg {

Status PageGroupedDB::Open(const PageGroupedDBOptions& options,
                           const std::filesystem::path& path,
                           PageGroupedDB** db_out) {
  return Status::OK();
}

Status PageGroupedDBImpl::BulkLoad(const std::vector<Record>& records) {
  return Status::OK();
}

Status PageGroupedDBImpl::Put(const Key key, const Slice& value) {
  return Status::OK();
}

Status PageGroupedDBImpl::Get(const Key key, std::string* value_out) {
  return Status::OK();
}

Status PageGroupedDBImpl::GetRange(
    const Key start_key, const size_t num_records,
    std::vector<std::pair<Key, std::string>>* results_out) {
  return Status::OK();
}

}  // namespace pg
}  // namespace llsm
