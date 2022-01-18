#pragma once

#include <algorithm>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "llsm/slice.h"
#include "manager.h"
#include "ycsbr/ycsbr.h"

namespace llsm {
namespace pg {

// Used to run YCSBR-generated workloads against the page grouping prototype
// directly.
class PageGroupingInterface {
 public:
  // Called once by each worker thread **before** the database is initialized.
  // Note that this method will be called concurrently by each worker thread.
  void InitializeWorker(const std::thread::id& worker_id) {}

  // Called once by each worker thread after it is done running. This method is
  // called concurrently by each worker thread and may run concurrently with
  // `DeleteDatabase()`.
  void ShutdownWorker(const std::thread::id& worker_id) {}

  // Called once before the benchmark.
  // Put any needed initialization code in here.
  void InitializeDatabase() {}

  // Called once if `InitializeDatabase()` has been called.
  // Put any needed clean up code in here.
  void ShutdownDatabase() {}

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    std::vector<std::pair<ycsbr::Request::Key, Slice>> records;
    records.reserve(load.size());
    for (const auto& rec : load) {
      records.emplace_back(rec.key, llsm::Slice(rec.value, rec.value_size));
    }
    std::sort(records.begin(), records.end(),
              [](const std::pair<ycsbr::Request::Key, const Slice>& r1,
                 const std::pair<ycsbr::Request::Key, const Slice>& r2) {
                return r1.first < r2.first;
              });

    // Temporary hook-in for testing.
    Manager::LoadIntoNew(".", records, Manager::LoadOptions());
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return false;
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return false;
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) { return false; }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      ycsbr::Request::Key key, size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    return false;
  }
};

}  // namespace pg
}  // namespace llsm
