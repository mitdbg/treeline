#pragma once

#include <cstdint>
#include <string>
#include <thread>

#include "config.h"
#include "treeline/db.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

class TreeLineInterface {
 public:
  TreeLineInterface() : db_(nullptr), min_key_(0), max_key_(0), num_keys_(1) {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  // Set the key distribution hints needed by TreeLine to start up.
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {
    min_key_ = min_key;
    max_key_ = max_key;
    num_keys_ = num_keys;
  }

  void WriteOutStats(const std::filesystem::path& out_dir) {}

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/tl";
    tl::Options options = tl::bench::BuildTreeLineOptions();
    options.key_hints.num_keys = 0;  // Needs to be empty to use bulk load.
    if (num_keys_ <= 1) {
      // We set the step size to at least 1 to ensure any code that relies on
      // the step size to generate values does not end up in an infinite loop.
      options.key_hints.key_step_size = 1;
    } else {
      // Set `key_step_size` to the smallest integer where
      // `min_key_ + key_step_size * (num_keys_ - 1) >= max_key_` holds.
      const size_t diff = max_key_ - min_key_;
      const size_t denom = (num_keys_ - 1);
      options.key_hints.key_step_size =
          (diff / denom) + (diff % denom != 0);  // Computes ceil(diff/denom)
    }

    if (FLAGS_verbose) {
      std::cerr << "> TreeLine memtable flush threshold: "
                << options.memtable_flush_threshold << " bytes" << std::endl;
      std::cerr << "> TreeLine buffer pool size: " << options.buffer_pool_size
                << " bytes" << std::endl;
      std::cerr << "> Opening TreeLine at " << dbname << std::endl;
    }

    tl::Status status = tl::DB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start TreeLine: " +
                               status.ToString());
    }
  }

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {
    if (db_ == nullptr) {
      return;
    }
    delete db_;
    db_ = nullptr;
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    std::vector<tl::key_utils::IntKeyAsSlice> keys;
    std::vector<std::pair<const tl::Slice, const tl::Slice>> records;
    keys.reserve(load.size());
    records.reserve(load.size());
    for (const auto& req : load) {
      keys.emplace_back(req.key);
      records.emplace_back(keys.back().as<tl::Slice>(),
                           tl::Slice(req.value, req.value_size));
    }

    tl::WriteOptions options;
    options.bypass_wal = FLAGS_bypass_wal;
    options.sorted_load = true;

    tl::Status s = db_->BulkLoad(options, records);

    if (!s.ok()) {
      throw std::runtime_error("Failed to bulk load records!");
    }
    db_->FlushRecordCache(/*disable_deferred_io = */ true);
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return Insert(key, value, value_size);
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    tl::WriteOptions options;
    options.bypass_wal = FLAGS_bypass_wal;
    tl::Status status =
        db_->Put(options, strkey.as<tl::Slice>(), tl::Slice(value, value_size));
    return status.ok();
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    const tl::ReadOptions options;
    tl::Status status = db_->Get(options, strkey.as<tl::Slice>(), value_out);
    return status.ok();
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    scan_out->clear();
    scan_out->reserve(amount);
    const tl::key_utils::IntKeyAsSlice strkey(key);
    const tl::ReadOptions options;
    tl::RecordBatch results;
    tl::Status status =
        db_->GetRange(options, strkey.as<tl::Slice>(), amount, &results);
    for (auto& record : results) {
      scan_out->emplace_back(tl::key_utils::ExtractHead64(record.key()),
                             std::move(record).ExtractValue());
    }
    return status.ok();
  }

 private:
  tl::DB* db_;

  // These variables are used to provide hints about the key distribution to
  // TreeLine when creating a new database. We need these hints because TreeLine
  // currently does not support adjusting itself to a changing key distribution.
  uint64_t min_key_, max_key_, num_keys_;
};
