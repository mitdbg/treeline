#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "db/format.h"
#include "manager.h"
#include "record_cache/record_cache.h"
#include "treeline/pg_db.h"
#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "util/insert_tracker.h"

namespace tl {
namespace pg {

class PageGroupedDBImpl : public PageGroupedDB {
 public:
  PageGroupedDBImpl(std::filesystem::path db_path, PageGroupedDBOptions options,
                    std::optional<Manager> mgr);
  ~PageGroupedDBImpl() override;

  PageGroupedDBImpl(const PageGroupedDBImpl&) = delete;
  PageGroupedDBImpl& operator=(const PageGroupedDBImpl&) = delete;

  Status BulkLoad(const std::vector<Record>& records) override;
  Status Put(const WriteOptions& options, const Key key,
             const Slice& value) override;
  Status Get(const Key key, std::string* value_out) override;
  Status GetRange(const Key start_key, const size_t num_records,
                  std::vector<std::pair<Key, std::string>>* results_out,
                  bool use_experimental_prefetch = false) override;

  Status FlattenRange(
      const Key start_key = 1,
      const Key end_key = std::numeric_limits<Key>::max()) override;

 private:
  void WriteBatch(const WriteOutBatch& records);
  std::pair<Key, Key> GetPageBoundsFor(Key key);

  std::filesystem::path db_path_;
  PageGroupedDBOptions options_;

  // After opening a "new" DB, this will be empty until `BulkLoad()` is called.
  // Otherwise this is always non-empty.
  std::optional<Manager> mgr_;
  RecordCache cache_;

  std::shared_ptr<InsertTracker> tracker_;
};

}  // namespace pg
}  // namespace tl
