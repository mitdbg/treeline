#pragma once

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include "config.h"
#include "rocksdb/db.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

// Used with YCSBR to run end-to-end benchmarks against RocksDB.
class RocksDBInterface {
 public:
  RocksDBInterface() : db_(nullptr) {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {}
  void WriteOutStats(const std::filesystem::path& out_dir) {
    std::ofstream out(out_dir / "counters.csv");
    out << "memtable_hit, memtable_miss, block_cache_index_hit, "
           "block_cache_index_miss, "
           "block_cache_filter_hit, block_cache_filter_miss, "
           "block_cache_data_hit, block_cache_data_miss"
        << std::endl;

    out << options_.statistics->getTickerCount(rocksdb::MEMTABLE_HIT) << ","
        << options_.statistics->getTickerCount(rocksdb::MEMTABLE_MISS) << ","
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_HIT) << "," 
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_MISS) << ","
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_HIT) << "," 
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_MISS) << ","
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_DATA_HIT) << "," 
        << options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS) << ","
        << std::endl;
  }

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/rocksdb";
    options_ = tl::bench::BuildRocksDBOptions();
    options_.create_if_missing = true;
    if (FLAGS_verbose) {
      std::cerr << "> RocksDB memtable flush threshold: "
                << options_.write_buffer_size << " bytes" << std::endl;
      std::cerr << "> RocksDB block cache: " << FLAGS_cache_size_mib << " MiB"
                << std::endl;
      std::cerr << "> Opening RocksDB DB at " << dbname << std::endl;
    }

    rocksdb::Status status = rocksdb::DB::Open(options_, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start RocksDB: " + status.ToString());
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
  void BulkLoad(const ycsbr::BulkLoadTrace& records) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_bypass_wal;
    rocksdb::Status status;
    rocksdb::WriteBatch batch;
    constexpr size_t batch_size = 1024;

    for (const auto& rec : records) {
      const tl::key_utils::IntKeyAsSlice strkey(rec.key);
      status =
          batch.Put(strkey.as<rocksdb::Slice>(),
                    rocksdb::Slice(reinterpret_cast<const char*>(&(rec.value)),
                                   rec.value_size));
      if (!status.ok()) {
        throw std::runtime_error("Failed to write record to RocksDB instance.");
      }
      if (batch.Count() >= batch_size) {
        status = db_->Write(options, &batch);
        if (!status.ok()) {
          throw std::runtime_error("Failed to write batch to RocksDB.");
        }
        batch.Clear();
      }
    }
    if (batch.Count() > 0) {
      status = db_->Write(options, &batch);
      if (!status.ok()) {
        throw std::runtime_error("Failed to write batch to RocksDB.");
      }
      batch.Clear();
    }

    rocksdb::CompactRangeOptions compact_options;
    compact_options.change_level = true;
    db_->CompactRange(compact_options, nullptr, nullptr);
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    rocksdb::ReadOptions options;
    // See
    // https://github.com/facebook/rocksdb/wiki/Prefix-Seek#adaptive-prefix-mode
    options.auto_prefix_mode = true;
    auto status = db_->Get(options, strkey.as<rocksdb::Slice>(), value_out);
    return status.ok();
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_bypass_wal;
    auto status = db_->Put(options, strkey.as<rocksdb::Slice>(),
                           rocksdb::Slice(value, value_size));
    return status.ok();
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return Insert(key, value, value_size);
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    scan_out->clear();
    scan_out->reserve(amount);
    const tl::key_utils::IntKeyAsSlice strkey(key);
    rocksdb::ReadOptions options;
    // See
    // https://github.com/facebook/rocksdb/wiki/Prefix-Seek#adaptive-prefix-mode
    options.auto_prefix_mode = true;
    rocksdb::Iterator* it = db_->NewIterator(options);
    it->Seek(strkey.as<rocksdb::Slice>());
    while (it->Valid() && scan_out->size() < amount) {
      scan_out->emplace_back(tl::key_utils::ExtractHead64(it->key()),
                             it->value().ToString());
      it->Next();
    }
    delete it;
    return true;
  }

 private:
  rocksdb::DB* db_;
  rocksdb::Options options_;
};
