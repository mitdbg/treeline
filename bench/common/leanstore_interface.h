#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "config.h"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/LeanStore.hpp"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

class LeanStoreInterface {
 public:
  LeanStoreInterface() : db_(nullptr) {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {}
  void WriteOutStats(const std::filesystem::path& out_dir) {}

  // Called once before the benchmark.
  void InitializeDatabase() {
    // LeanStore relies on gflags for its configuration. This is a quick way to
    // make their configuration assumptions compatible with our configuration
    // assumptions.
    FLAGS_ssd_path = FLAGS_db_path + "/leanstore/dbfile";
    FLAGS_dram_gib =
        (2 * FLAGS_memtable_size_mib + FLAGS_cache_size_mib) / 1024.0;
    FLAGS_wal = !FLAGS_bypass_wal;
    FLAGS_pp_threads = FLAGS_bg_threads;
    if (FLAGS_skip_load) {
      FLAGS_recover = true;
      FLAGS_recover_file = FLAGS_db_path + "/leanstore/leanstore.json";
    } else {
      FLAGS_falloc = 2;  // GiB
      FLAGS_persist = true;
      FLAGS_persist_file = FLAGS_db_path + "/leanstore/leanstore.json";
    }

    if (!std::filesystem::exists(FLAGS_ssd_path)) {
      std::filesystem::create_directory(FLAGS_db_path + "/leanstore");
      // LeanStore requires the on-disk file to actually exist before starting
      // up (it seems like it can be empty).
      std::ofstream leanstore_file(FLAGS_ssd_path, std::ofstream::app);
    }
    db_ = new leanstore::LeanStore();

    if (FLAGS_skip_load) {
      table_ = &db_->retrieveBTreeLL("btree");
    } else {
      table_ = &db_->registerBTreeLL("btree");
    }
    std::cerr << table_ << std::endl;
  }

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {
    if (db_ == nullptr) {
      return;
    }
    // Persist changes to disk to be able to measure space usage.
    // NOTE: LeanStore does not support "reopening" a persisted database. So
    // doing this is primarily useful only for database size measurements.
    db_->getBufferManager().writeAllBufferFrames();
    delete db_;
    db_ = nullptr;
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    for (const auto& req : load) {
      if (!Insert(req.key, req.value, req.value_size)) {
        throw std::runtime_error("Failed to bulk load a record!");
      }
    }
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    auto result = table_->updateSameSize(
        reinterpret_cast<uint8_t*>(
            const_cast<char*>(strkey.as<tl::Slice>().data())),
        strkey.as<tl::Slice>().size(), [&](u8* payload, u16 payload_length) {
          memcpy(payload, value, payload_length);
        });
    return (result == leanstore::storage::btree::OP_RESULT::OK);
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    auto result = table_->insert(
        reinterpret_cast<uint8_t*>(
            const_cast<char*>(strkey.as<tl::Slice>().data())),
        strkey.as<tl::Slice>().size(),
        reinterpret_cast<uint8_t*>(const_cast<char*>(value)), value_size);
    return (result == leanstore::storage::btree::OP_RESULT::OK);
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    auto result =
        table_->lookup(reinterpret_cast<uint8_t*>(
                           const_cast<char*>(strkey.as<tl::Slice>().data())),
                       strkey.as<tl::Slice>().size(),
                       [&](const u8* payload, u16 payload_length) {
                         value_out->resize(payload_length);
                         memcpy(value_out->data(), payload, payload_length);
                       });
    return (result == leanstore::storage::btree::OP_RESULT::OK);
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    size_t scanned = 0;

    auto result = table_->scanAsc(
        reinterpret_cast<uint8_t*>(
            const_cast<char*>(strkey.as<tl::Slice>().data())),
        strkey.as<tl::Slice>().size(),
        [&](const u8* key, u16 key_length, const u8* payload,
            u16 payload_length) {
          if (scanned++ >= amount) {
            return false;
          }

          assert(key_length == sizeof(ycsbr::Request::Key));
          scan_out->emplace_back(
              __builtin_bswap64(*reinterpret_cast<const uint64_t*>(key)),
              std::string(reinterpret_cast<const char*>(payload),
                          payload_length));
          return true;
        },
        [&]() { scanned = 0; });

    // Need to also accept `OP_RESULT::NOT_FOUND` because we might try to get
    // past the last db key while scanning.
    return (result == leanstore::storage::btree::OP_RESULT::OK ||
            result == leanstore::storage::btree::OP_RESULT::NOT_FOUND);
  }

 private:
  leanstore::LeanStore* db_;
  leanstore::storage::btree::BTreeLL* table_;
};
