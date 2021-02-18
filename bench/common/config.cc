#include "config.h"

#include <iostream>
#include <unordered_map>

#include "db/page.h"
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

DEFINE_string(db_path, "", "The path where the database(s) should be stored.");
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
    io_threshold, 1,
    "The minimum number of operations to a given page that need to be "
    "encoutered while flushing a memtable in order to trigger a flush");
DEFINE_uint64(max_deferrals, 0,
              "The maximum number of times that a given operation can be "
              "deferred to a future flush.");

DEFINE_bool(bypass_wal, true,
            "If true, all writes will bypass the write-ahead log.");

namespace llsm {
namespace bench {

std::optional<DBType> ParseDBType(const std::string& candidate) {
  static std::unordered_map<std::string, DBType> kStringToDBType = {
      {"all", DBType::kAll},
      {"llsm", DBType::kLLSM},
      {"rocksdb", DBType::kRocksDB}};

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
  options.record_size = FLAGS_record_size_bytes;
  options.page_fill_pct = FLAGS_llsm_page_fill_pct;
  options.pin_threads = true;
  options.deferred_io_min_entries = FLAGS_io_threshold;
  options.deferred_io_max_deferrals = FLAGS_max_deferrals;
  return options;
}

}  // namespace bench
}  // namespace llsm
