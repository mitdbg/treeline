#pragma once

#include <filesystem>
#include <optional>

#include "llsm/pg_db.h"
#include "llsm/pg_options.h"
#include "manager.h"

namespace llsm {
namespace pg {

class PageGroupedDBImpl : public PageGroupedDB {
 public:
  PageGroupedDBImpl(std::filesystem::path db_path, PageGroupedDBOptions options,
                    std::optional<Manager> mgr);

  PageGroupedDBImpl(const PageGroupedDBImpl&) = delete;
  PageGroupedDBImpl& operator=(const PageGroupedDBImpl&) = delete;

  Status BulkLoad(const std::vector<Record>& records) override;
  Status Put(const Key key, const Slice& value) override;
  Status Get(const Key key, std::string* value_out) override;
  Status GetRange(
      const Key start_key, const size_t num_records,
      std::vector<std::pair<Key, std::string>>* results_out) override;

 private:
  // After opening a "new" DB, this will be empty until `BulkLoad()` is called.
  // Otherwise this is always non-empty.
  std::optional<Manager> mgr_;

  std::filesystem::path db_path_;
  PageGroupedDBOptions options_;
};

}  // namespace pg
}  // namespace llsm
