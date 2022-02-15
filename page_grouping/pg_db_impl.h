#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "db/format.h"
#include "llsm/pg_db.h"
#include "llsm/pg_options.h"
#include "llsm/slice.h"
#include "manager.h"
#include "record_cache/record_cache.h"

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
  void WriteBatch(
      const std::vector<std::tuple<Slice, Slice, format::WriteType>>& records);

  // After opening a "new" DB, this will be empty until `BulkLoad()` is called.
  // Otherwise this is always non-empty.
  std::optional<Manager> mgr_;
  RecordCache cache_;

  std::filesystem::path db_path_;
  PageGroupedDBOptions options_;
};

}  // namespace pg
}  // namespace llsm
