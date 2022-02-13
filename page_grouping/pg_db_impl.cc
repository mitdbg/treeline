#include "pg_db_impl.h"

namespace fs = std::filesystem;

namespace llsm {
namespace pg {

Status PageGroupedDB::Open(const PageGroupedDBOptions& options,
                           const std::filesystem::path& db_path,
                           PageGroupedDB** db_out) {
  // TODO: This open logic could be improved, but it is good enough for our
  // current use cases.
  if (std::filesystem::exists(db_path) &&
      std::filesystem::is_directory(db_path) &&
      !std::filesystem::is_empty(db_path)) {
    // Reopening an existing database.
    Manager mgr = Manager::Reopen(db_path, options);
    *db_out = new PageGroupedDBImpl(db_path, options, std::move(mgr));
  } else {
    // Opening a new database.
    *db_out = new PageGroupedDBImpl(db_path, options, std::optional<Manager>());
  }
  return Status::OK();
}

PageGroupedDBImpl::PageGroupedDBImpl(fs::path db_path,
                                     PageGroupedDBOptions options,
                                     std::optional<Manager> mgr)
    : db_path_(std::move(db_path)),
      options_(std::move(options)),
      mgr_(std::move(mgr)) {}

Status PageGroupedDBImpl::BulkLoad(const std::vector<Record>& records) {
  if (mgr_.has_value()) {
    return Status::InvalidArgument("Cannot bulk load a non-empty DB.");
  }
  mgr_ = Manager::LoadIntoNew(db_path_, records, options_);
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
