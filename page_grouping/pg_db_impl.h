#pragma once

#include "llsm/pg_db.h"
#include "llsm/pg_options.h"
#include "manager.h"

namespace llsm {
namespace pg {

class PageGroupedDBImpl : public PageGroupedDB {
 public:
  explicit PageGroupedDBImpl(Manager mgr);

  PageGroupedDBImpl(const PageGroupedDBImpl&) = delete;
  PageGroupedDBImpl& operator=(const PageGroupedDBImpl&) = delete;

  Status BulkLoad(const std::vector<Record>& records) override;
  Status Put(const Key key, const Slice& value) override;
  Status Get(const Key key, std::string* value_out) override;
  Status GetRange(
      const Key start_key, const size_t num_records,
      std::vector<std::pair<Key, std::string>>* results_out) override;

 private:
  Manager mgr_;
};

}  // namespace pg
}  // namespace llsm
