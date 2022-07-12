#pragma once

#include <algorithm>
#include <cassert>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "config.h"
#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "manager.h"
#include "ycsbr/ycsbr.h"

namespace tl {
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
  void ShutdownWorker(const std::thread::id& worker_id) {
    if (!pg_mgr_.has_value()) return;

    std::unique_lock<std::mutex> lock(mutex_);
    if (read_counts_.empty()) {
      read_counts_ = pg_mgr_->GetReadCounts();
    } else {
      const auto& local_read_counts = pg_mgr_->GetReadCounts();
      assert(read_counts_.size() == local_read_counts.size());
      for (size_t i = 0; i < local_read_counts.size(); ++i) {
        read_counts_[i] += local_read_counts[i];
      }
    }
    if (write_counts_.empty()) {
      write_counts_ = pg_mgr_->GetWriteCounts();
    } else {
      const auto& local_write_counts = pg_mgr_->GetWriteCounts();
      assert(write_counts_.size() == local_write_counts.size());
      for (size_t i = 0; i < local_write_counts.size(); ++i) {
        write_counts_[i] += local_write_counts[i];
      }
    }
  }

  // Called once before the benchmark.
  // Put any needed initialization code in here.
  void InitializeDatabase() {
    db_path_ = std::filesystem::path(FLAGS_db_path);
    if (std::filesystem::exists(db_path_) &&
        std::filesystem::is_directory(db_path_) &&
        !std::filesystem::is_empty(db_path_)) {
      // Reopening an existing database.
      pg_mgr_ = Manager::Reopen(db_path_, GetOptions());
    } else {
      // No-op. Will initialize during bulk load.
    }
    write_batch_.reserve(FLAGS_write_batch_size);
  }

  // Called once if `InitializeDatabase()` has been called.
  // Put any needed clean up code in here.
  void ShutdownDatabase() {
    if (write_batch_.size() > 0) {
      SubmitWrites();
    }
    // Purposefully keep the `Manager` around for statistics aggregation in
    // `ShutdownWorker()`.
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    if (pg_mgr_.has_value()) {
      // Already initialized existing DB! Cannot bulk load.
      throw std::runtime_error(
          "DB already exists! Bulk load is not supported.");
    }
    std::vector<std::pair<ycsbr::Request::Key, Slice>> records;
    records.reserve(load.size());
    for (const auto& rec : load) {
      records.emplace_back(rec.key, tl::Slice(rec.value, rec.value_size));
    }
    std::sort(records.begin(), records.end(),
              [](const std::pair<ycsbr::Request::Key, const Slice>& r1,
                 const std::pair<ycsbr::Request::Key, const Slice>& r2) {
                return r1.first < r2.first;
              });

    pg_mgr_ = Manager::LoadIntoNew(db_path_, records, GetOptions());
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return false;
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    write_batch_.emplace_back(key, Slice(value, value_size));
    if (write_batch_.size() >= FLAGS_write_batch_size) {
      SubmitWrites();
    }
    return true;
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    return pg_mgr_->Get(key, value_out).ok();
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      ycsbr::Request::Key key, size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    return pg_mgr_->Scan(key, amount, scan_out).ok();
  }

  const std::vector<size_t>& GetReadCounts() const { return read_counts_; }

  const std::vector<size_t>& GetWriteCounts() const { return write_counts_; }

 private:
  PageGroupedDBOptions GetOptions() {
    PageGroupedDBOptions options;
    options.records_per_page_goal = FLAGS_records_per_page_goal;
    options.records_per_page_epsilon = FLAGS_records_per_page_epsilon;
    options.use_segments = !FLAGS_disable_segments;
    options.num_bg_threads = FLAGS_bg_threads;
    options.use_memory_based_io = FLAGS_use_memory_based_io;
    return options;
  }

  void SubmitWrites() {
    std::sort(write_batch_.begin(), write_batch_.end(),
              [](const auto& left, const auto& right) {
                return left.first < right.first;
              });
    pg_mgr_->PutBatch(write_batch_);
    write_batch_.clear();
  }

  std::filesystem::path db_path_;
  std::optional<Manager> pg_mgr_;

  std::vector<std::pair<ycsbr::Request::Key, Slice>> write_batch_;

  // Combined read/write counts from all worker threads.
  std::mutex mutex_;
  std::vector<size_t> read_counts_, write_counts_;
};

}  // namespace pg
}  // namespace tl
