#include "config.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unordered_map>

#include "db/page.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"

namespace {

bool EnsureNonZero(const char* flagname, uint32_t value) {
  if (value != 0) return true;
  std::cerr << "ERROR: --" << flagname << " must be non-zero." << std::endl;
  return false;
}

bool EnsureNonEmpty(const char* flagname, const std::string& value) {
  if (!value.empty()) return true;
  std::cerr << "ERROR: --" << flagname << " must be non-empty." << std::endl;
  return false;
}

bool ValidateDB(const char* flagname, const std::string& db) {
  if (llsm::bench::ParseDBType(db).has_value()) return true;
  std::cerr << "ERROR: Unknown DB type: " << db << std::endl;
  return false;
}

bool ValidateRecordSize(const char* flagname, uint32_t record_size) {
  if (record_size >= 9) return true;
  std::cerr << "ERROR: --record_size_bytes must be at least 9 (8 byte key + 1 "
               "byte value)."
            << std::endl;
  return false;
}

bool ValidateBGThreads(const char* flagname, uint32_t bg_threads) {
  if (bg_threads >= 2) return true;
  std::cerr << "ERROR: --bg_threads must be at least 2 (LLSM needs at least 2 "
               "background threads)."
            << std::endl;
  return false;
}

bool ValidateLLSMPageFillPct(const char* flagname, uint32_t pct) {
  if (pct >= 1 && pct <= 100) return true;
  std::cerr << "ERROR: --llsm_page_fill_pct must be a value between 1 and 100 "
               "inclusive."
            << std::endl;
  return false;
}

}  // namespace

DEFINE_string(db, "all", "Which database(s) to use {all, rocksdb, llsm}.");
DEFINE_validator(db, &ValidateDB);

DEFINE_string(db_path, llsm::bench::GetDefaultDBPath(),
              "The path where the database(s) should be stored.");
DEFINE_validator(db_path, &EnsureNonEmpty);

DEFINE_uint32(trials, 1, "The number of times to repeat the experiment.");
DEFINE_validator(trials, &EnsureNonZero);

DEFINE_uint32(seed, 42,
              "The seed to use for the PRNG (to ensure reproducibility).");

DEFINE_uint32(record_size_bytes, 16, "The size of each record, in bytes.");
DEFINE_validator(record_size_bytes, &ValidateRecordSize);

DEFINE_uint64(cache_size_mib, 64,
              "The size of the database's in memory cache, in MiB.");

DEFINE_uint32(bg_threads, 2,  // LLSM needs at least 2 background threads.
              "The number background threads that RocksDB/LLSM should use.");
DEFINE_validator(bg_threads, &ValidateBGThreads);

DEFINE_bool(use_direct_io, true, "Whether or not to use direct I/O.");

DEFINE_uint64(memtable_size_mib, 64,
              "The size of the memtable before it should be flushed, in MiB.");

DEFINE_uint32(llsm_page_fill_pct, 50,
              "How full each LLSM page should be, as a value between 1 and 100 "
              "inclusive.");
DEFINE_validator(llsm_page_fill_pct, &ValidateLLSMPageFillPct);

DEFINE_uint64(
    io_min_batch_size, 1,
    "The minimum size of a batch for a given page (in bytes) that must be"
    "encoutered while flushing a memtable in order to trigger a flush");
DEFINE_uint64(max_deferrals, 0,
              "The maximum number of times that a given operation can be "
              "deferred to a future flush.");
DEFINE_bool(deferral_autotuning, false,
            "Whether or not to auto-tune deferral parameters");
DEFINE_bool(memory_autotuning, false,
            "Whether or not to auto-tune memory allocation");

DEFINE_bool(bypass_wal, true,
            "If true, all writes will bypass the write-ahead log.");

DEFINE_bool(verbose, false,
            "If set, benchmark information will be printed to stderr.");

DEFINE_uint32(latency_sample_period, 1,
              "The number of requests between latency measurements (i.e., "
              "measure latency every N-th request).");
DEFINE_validator(latency_sample_period, &EnsureNonZero);

DEFINE_bool(use_alex, true,
            "If true, LLSM will use an ALEXModel. Otherwise, it will use a "
            "BTreeModel.");

DEFINE_uint32(rdb_bloom_bits, 0,
              "The number of bloom filter bits to use in RocksDB. Set to 0 to "
              "disable the use of bloom filters.");
DEFINE_uint32(rdb_prefix_bloom_size, 0,
              "The number of bytes to include in a prefix bloom filter. This "
              "is only used when bloom filters are enabled (see the flag "
              "above). Set to 0 to disable the use of prefix bloom filters.");

// The minimum length of an overflow chain for which reorganization is
// triggered.
DEFINE_uint64(reorg_length, 5,
              "The minimum length of an overflow chain for which "
              "reorganization is triggered.");

// Page grouping related flags.

DEFINE_uint64(records_per_page_goal, 45, "Page grouping fill rate goal.");
DEFINE_uint64(records_per_page_delta, 5,
              "Page grouping model error tolerance.");
DEFINE_bool(pg_use_segments, true,
            "If set to false, all segments will be a single page (emulates not "
            "using page grouping).");
DEFINE_bool(pg_use_memory_based_io, false,
            "If set, PGLLSM will use memory-based I/O (only meant for setup; "
            "not for use during evaluation).");
DEFINE_bool(pg_bypass_cache, false,
            "If set, PGLLSM will bypass the record cache. All requests will "
            "incur I/O.");

namespace llsm {
namespace bench {

std::optional<DBType> ParseDBType(const std::string& candidate) {
  static const std::unordered_map<std::string, DBType> kStringToDBType = {
      {"all", DBType::kAll},         {"llsm", DBType::kLLSM},
      {"rocksdb", DBType::kRocksDB}, {"leanstore", DBType::kLeanStore},
      {"kvell", DBType::kKVell},     {"pg_llsm", DBType::kPGLLSM}};

  auto it = kStringToDBType.find(candidate);
  if (it == kStringToDBType.end()) {
    return std::optional<DBType>();
  }
  return it->second;
}

rocksdb::Options BuildRocksDBOptions() {
  rocksdb::Options options;
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.use_direct_reads = FLAGS_use_direct_io;
  options.use_direct_io_for_flush_and_compaction = FLAGS_use_direct_io;
  options.write_buffer_size = FLAGS_memtable_size_mib * 1024 * 1024;
  options.IncreaseParallelism(FLAGS_bg_threads);

  rocksdb::LRUCacheOptions cache_options;
  cache_options.capacity = FLAGS_cache_size_mib * 1024 * 1024;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size =
      Page::kSize;  // Use the same block size as LLSM's pages.
  table_options.checksum = rocksdb::kNoChecksum;
  table_options.block_cache = rocksdb::NewLRUCache(cache_options);
  if (FLAGS_rdb_bloom_bits > 0) {
    if (FLAGS_verbose) {
      std::cerr << "> RocksDB using bloom filters with " << FLAGS_rdb_bloom_bits
                << " bits." << std::endl;
    }
    table_options.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(FLAGS_rdb_bloom_bits, false));

    if (FLAGS_rdb_prefix_bloom_size > 0) {
      options.prefix_extractor.reset(
          rocksdb::NewCappedPrefixTransform(FLAGS_rdb_prefix_bloom_size));
      if (FLAGS_verbose) {
        std::cerr
            << "> RocksDB using prefix bloom filters with a prefix of size "
            << FLAGS_rdb_prefix_bloom_size << std::endl;
      }
    } else if (FLAGS_verbose) {
      std::cerr << "> RocksDB is NOT using prefix bloom filters." << std::endl;
    }
  } else if (FLAGS_verbose) {
    std::cerr << "> RocksDB is NOT using bloom filters." << std::endl;
  }
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));

  return options;
}

llsm::Options BuildLLSMOptions() {
  llsm::Options options;
  options.buffer_pool_size = FLAGS_cache_size_mib * 1024 * 1024;
  options.memtable_flush_threshold = FLAGS_memtable_size_mib * 1024 * 1024;
  options.use_direct_io = FLAGS_use_direct_io;
  options.background_threads = FLAGS_bg_threads;
  options.key_hints.record_size = FLAGS_record_size_bytes;
  options.key_hints.page_fill_pct = FLAGS_llsm_page_fill_pct;
  options.pin_threads = true;
  options.deferred_io_batch_size = FLAGS_io_min_batch_size;
  options.deferred_io_max_deferrals = FLAGS_max_deferrals;
  options.use_alex = FLAGS_use_alex;
  options.deferral_autotuning = FLAGS_deferral_autotuning;
  options.memory_autotuning = FLAGS_memory_autotuning;
  options.reorg_length = FLAGS_reorg_length;
  return options;
}

llsm::pg::PageGroupedDBOptions BuildPGLLSMOptions() {
  llsm::pg::PageGroupedDBOptions options;
  options.use_segments = FLAGS_pg_use_segments;
  options.records_per_page_goal = FLAGS_records_per_page_goal;
  options.records_per_page_delta = FLAGS_records_per_page_delta;
  options.num_bg_threads = FLAGS_bg_threads;
  options.record_cache_capacity =
      (FLAGS_cache_size_mib * 1024 * 1024) / (FLAGS_record_size_bytes);
  options.use_memory_based_io = FLAGS_pg_use_memory_based_io;
  options.bypass_cache = FLAGS_pg_bypass_cache;
  return options;
}

std::string AppendTimestamp(const std::string& prefix) {
  std::stringstream output;
  output << prefix << "+";
  const auto now = std::chrono::system_clock::now();
  const auto now_time_t = std::chrono::system_clock::to_time_t(now);
  output << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d+%H-%M-%S");
  return output.str();
}

std::string GetDefaultOutputPath() { return AppendTimestamp("llsm-out"); }

std::string GetDefaultDBPath() { return AppendTimestamp("llsm-bench-db"); }

}  // namespace bench
}  // namespace llsm
