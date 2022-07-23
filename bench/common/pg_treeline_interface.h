#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

#include "config.h"
#include "treeline/pg_db.h"
#include "treeline/pg_stats.h"
#include "util/key.h"
#include "ycsbr/ycsbr.h"

class PGTreeLineInterface {
 public:
  PGTreeLineInterface() : db_(nullptr) {}

  void InitializeWorker(const std::thread::id& id) {
    tl::pg::PageGroupedDBStats::Local().Reset();
  }

  void ShutdownWorker(const std::thread::id& id) {
    tl::pg::PageGroupedDBStats::Local().PostToGlobal();
  }

  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {
    // Unused - kept for use with `run_custom`.
  }

  void WriteOutStats(const std::filesystem::path& out_dir) {
    std::ofstream out(out_dir / "counters.csv");
    out << "name,value" << std::endl;
    tl::pg::PageGroupedDBStats::RunOnGlobal([&out](const auto& stats) {
      // clang-format off
      out << "cache_hits," << stats.GetCacheHits() << std::endl;
      out << "cache_misses," << stats.GetCacheMisses() << std::endl;
      out << "cache_clean_evictions," << stats.GetCacheCleanEvictions() << std::endl;
      out << "cache_dirty_evictions," << stats.GetCacheDirtyEvictions() << std::endl;

      out << "overflows_created," << stats.GetOverflowsCreated() << std::endl;
      out << "rewrites," << stats.GetRewrites() << std::endl;
      out << "rewrite_input_pages," << stats.GetRewriteInputPages() << std::endl;
      out << "rewrite_output_pages," << stats.GetRewriteOutputPages() << std::endl;

      out << "segments," << stats.GetSegments() << std::endl;
      out << "segment_index_bytes," << stats.GetSegmentIndexBytes() << std::endl;
      out << "free_list_entries," << stats.GetFreeListEntries() << std::endl;
      out << "free_list_bytes," << stats.GetFreeListBytes() << std::endl;
      out << "cache_bytes," << stats.GetCacheBytes() << std::endl;

      out << "overfetched_pages," << stats.GetOverfetchedPages() << std::endl;
      // clang-format on
    });
  }

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::filesystem::path dbname = GetDBPath(FLAGS_db_path);
    auto options = tl::bench::BuildPGTreeLineOptions();
    if (options.use_memory_based_io) {
      std::cerr << "> WARNING: PGTreeLine is using \"memory-based I/O\". "
                   "Performance results may be inflated."
                << std::endl;
    }
    if (FLAGS_verbose) {
      std::cerr << "> PGTreeLine using segments: "
                << (options.use_segments ? "true" : "false") << std::endl;
      std::cerr << "> PGTreeLine record cache size (# records): "
                << options.record_cache_capacity << std::endl;
      std::cerr << "> PGTreeLine records per page goal: "
                << options.records_per_page_goal << std::endl;
      std::cerr << "> PGTreeLine records per page epsilon: "
                << options.records_per_page_epsilon << std::endl;
      if (options.use_pgm_builder) {
        std::cerr << "> PGTreeLine is using the PGM linear model builder."
                  << std::endl;
      } else {
        std::cerr << "> PGTreeLine is using GreedyPLR." << std::endl;
      }
      std::cerr << "> Opening PGTreeLine DB at " << dbname << std::endl;
    }

    tl::pg::PageGroupedDBStats::RunOnGlobal(
        [](auto& global_stats) { global_stats.Reset(); });
    const tl::Status status =
        tl::pg::PageGroupedDB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start PGTreeLine: " +
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
    std::vector<tl::pg::Record> records;
    records.reserve(load.size());
    for (const auto& req : load) {
      const tl::key_utils::IntKeyAsSlice strkey(req.key);
      records.emplace_back(req.key, tl::Slice(req.value, req.value_size));
    }
    tl::Status s = db_->BulkLoad(records);
    if (!s.ok()) {
      throw std::runtime_error("Failed to bulk load records!");
    }
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    tl::pg::WriteOptions options;
    options.is_update = true;
    return db_->Put(options, key, tl::Slice(value, value_size)).ok();
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    tl::pg::WriteOptions options;
    options.is_update = false;
    return db_->Put(options, key, tl::Slice(value, value_size)).ok();
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
    return db_
        ->GetRange(key, amount, scan_out,
                   FLAGS_use_experimental_scan_prefetching)
        .ok();
  }

 private:
  // Returns a path to the PGTreeLine database checkpoint (used for
  // benchmarking).  This function helps with handling checkpoints saved using
  // the legacy name (pg_llsm).
  std::filesystem::path GetDBPath(
      const std::filesystem::path& checkpoint_path) {
    // If a DB checkpoint exists using our legacy name, then return it.
    // Otherwise use the new name.
    const std::string legacy_name = "pg_llsm";
    const std::filesystem::path legacy_path = checkpoint_path / legacy_name;
    if (std::filesystem::is_directory(legacy_path)) {
      return legacy_path;
    }

    const std::string expected_name = "pg_tl";
    return checkpoint_path / expected_name;
  }

  tl::pg::PageGroupedDB* db_;
};
