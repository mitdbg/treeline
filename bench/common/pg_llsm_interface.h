#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

#include "config.h"
#include "llsm/pg_db.h"
#include "llsm/pg_stats.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

class PGLLSMInterface {
 public:
  PGLLSMInterface() : db_(nullptr) {}

  void InitializeWorker(const std::thread::id& id) {
    llsm::pg::PageGroupedDBStats::Local().Reset();
  }

  void ShutdownWorker(const std::thread::id& id) {
    llsm::pg::PageGroupedDBStats::Local().PostToGlobal();
  }

  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {
    // Unused - kept for use with `run_custom`.
  }

  void WriteOutStats(const std::filesystem::path& out_dir) {
    std::ofstream out(out_dir / "counters.csv");
    out << "name,value" << std::endl;
    llsm::pg::PageGroupedDBStats::RunOnGlobal([&out](const auto& stats) {
      // clang-format off
      out << "cache_hits," << stats.GetCacheHits() << std::endl;
      out << "cache_misses," << stats.GetCacheMisses() << std::endl;
      out << "cache_clean_evictions," << stats.GetCacheCleanEvictions() << std::endl;
      out << "cache_dirty_evictions," << stats.GetCacheDirtyEvictions() << std::endl;

      out << "overflows_created," << stats.GetOverflowsCreated() << std::endl;
      out << "rewrites," << stats.GetRewrites() << std::endl;
      out << "rewritten_pages," << stats.GetRewrittenPages() << std::endl;

      out << "segments," << stats.GetSegments() << std::endl;
      out << "segment_index_bytes," << stats.GetSegmentIndexBytes() << std::endl;
      out << "free_list_entries," << stats.GetFreeListEntries() << std::endl;
      out << "free_list_bytes," << stats.GetFreeListBytes() << std::endl;
      out << "cache_bytes," << stats.GetCacheBytes() << std::endl;
      // clang-format on
    });
  }

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/pg_llsm";
    auto options = llsm::bench::BuildPGLLSMOptions();
    if (options.use_memory_based_io) {
      std::cerr << "> WARNING: PGLLSM is using \"memory-based I/O\". "
                   "Performance results may be inflated."
                << std::endl;
    }
    if (FLAGS_verbose) {
      std::cerr << "> PGLLSM using segments: "
                << (options.use_segments ? "true" : "false") << std::endl;
      std::cerr << "> PGLLSM record cache size (# records): "
                << options.record_cache_capacity << std::endl;
      std::cerr << "> PGLLSM records per page goal: "
                << options.records_per_page_goal << std::endl;
      std::cerr << "> PGLLSM records per page delta: "
                << options.records_per_page_delta << std::endl;
      std::cerr << "> Opening PGLLSM DB at " << dbname << std::endl;
    }

    llsm::pg::PageGroupedDBStats::RunOnGlobal(
        [](auto& global_stats) { global_stats.Reset(); });
    const llsm::Status status =
        llsm::pg::PageGroupedDB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start PGLLSM: " + status.ToString());
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
    std::vector<llsm::pg::Record> records;
    records.reserve(load.size());
    for (const auto& req : load) {
      const llsm::key_utils::IntKeyAsSlice strkey(req.key);
      records.emplace_back(req.key, llsm::Slice(req.value, req.value_size));
    }
    llsm::Status s = db_->BulkLoad(records);
    if (!s.ok()) {
      throw std::runtime_error("Failed to bulk load records!");
    }
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    llsm::pg::WriteOptions options;
    options.is_update = true;
    return db_->Put(options, key, llsm::Slice(value, value_size)).ok();
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    llsm::pg::WriteOptions options;
    options.is_update = false;
    return db_->Put(options, key, llsm::Slice(value, value_size)).ok();
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    return db_->Get(key, value_out).ok();
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    return db_->GetRange(key, amount, scan_out).ok();
  }

 private:
  llsm::pg::PageGroupedDB* db_;
};
