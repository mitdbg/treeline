#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bench/common/config.h"
#include "gflags/gflags.h"
#include "llsm/db.h"
#include "llsm/options.h"
#include "llsm/slice.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "ycsbr/ycsbr.h"

namespace {

namespace fs = std::filesystem;
using llsm::bench::DBType;

DEFINE_string(load_path, "", "Path to the bulk load workload file, if needed.");
DEFINE_string(workload_path, "", "Path to the workload file.");
DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_bool(verbose, false,
            "If set, benchmark information will be printed to stderr.");

class RocksDBInterface {
 public:
  RocksDBInterface() : db_(nullptr) {}

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/rocksdb";
    rocksdb::Options options = llsm::bench::BuildRocksDBOptions();
    options.create_if_missing = !FLAGS_load_path.empty();
    if (FLAGS_verbose) {
      std::cerr << "> RocksDB memtable flush threshold: "
                << options.write_buffer_size << " bytes" << std::endl;
      std::cerr << "> RocksDB block cache: " << FLAGS_cache_size_mib << " MiB"
                << std::endl;
      std::cerr << "> Opening RocksDB DB at " << dbname << std::endl;
    }

    rocksdb::Status status = rocksdb::DB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start RocksDB: " + status.ToString());
    }
  }

  // Called once after the workload if `InitializeDatabase()` has been called.
  void DeleteDatabase() {
    if (db_ == nullptr) {
      return;
    }
    delete db_;
    db_ = nullptr;
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadWorkload& records) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_bypass_wal;
    rocksdb::Status status;
    rocksdb::WriteBatch batch;
    constexpr size_t batch_size = 1024;

    for (const auto& rec : records) {
      status =
          batch.Put(rocksdb::Slice(reinterpret_cast<const char*>(&rec.key),
                                   sizeof(rec.key)),
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
    rocksdb::ReadOptions options;
    auto status = db_->Get(
        options,
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
        value_out);
    return status.ok();
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_bypass_wal;
    auto status = db_->Put(
        options,
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
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
      ycsbr::Request::Key key, size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    // Unimplemented.
    return false;
  }

 private:
  rocksdb::DB* db_;
};

class LLSMInterface {
 public:
  LLSMInterface() : db_(nullptr), min_key_(0), max_key_(0), num_keys_(1) {}

  // Set the key distribution hints needed by LLSM to start up.
  void SetKeyDistHints(uint64_t min_key, uint64_t max_key, uint64_t num_keys) {
    min_key_ = min_key;
    max_key_ = max_key;
    num_keys_ = num_keys;
  }

  // Called once before the benchmark.
  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/llsm";
    llsm::Options options = llsm::bench::BuildLLSMOptions();
    options.key_hints.num_keys = num_keys_;
    options.key_hints.min_key = min_key_;
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
      std::cerr << "> LLSM memtable flush threshold: "
                << options.memtable_flush_threshold << " bytes" << std::endl;
      std::cerr << "> LLSM buffer pool size: " << options.buffer_pool_size
                << " bytes" << std::endl;
      std::cerr << "> Opening LLSM DB at " << dbname << std::endl;
    }

    llsm::Status status = llsm::DB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start LLSM: " + status.ToString());
    }
  }

  // Called once after the workload if `InitializeDatabase()` has been called.
  void DeleteDatabase() {
    if (db_ == nullptr) {
      return;
    }
    delete db_;
    db_ = nullptr;
  }

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadWorkload& load) {
    for (const auto& req : load) {
      if (!Insert(req.key, req.value, req.value_size)) {
        throw std::runtime_error("Failed to bulk load a record!");
      }
    }
    db_->FlushMemTable(llsm::FlushOptions());
  }

  // Update the value at the specified key. Return true if the update succeeded.
  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return Insert(key, value, value_size);
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    llsm::WriteOptions options;
    options.bypass_wal = FLAGS_bypass_wal;
    llsm::Status status = db_->Put(
        options, llsm::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
        llsm::Slice(value, value_size));
    return status.ok();
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    llsm::ReadOptions options;
    llsm::Status status = db_->Get(
        options, llsm::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
        value_out);
    return status.ok();
  }

  // Scan the key range starting from `key` for `amount` records. Return true if
  // the scan succeeded.
  bool Scan(
      ycsbr::Request::Key key, size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    // Unimplemented.
    return false;
  }

 private:
  llsm::DB* db_;

  // These variables are used to provide hints about the key distribution to
  // LLSM when creating a new database. We need these hints because LLSM
  // currently does not support adjusting itself to a changing key distribution.
  // TODO: Remove these once LLSM can start up without requiring hints.
  uint64_t min_key_, max_key_, num_keys_;
};

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(DatabaseInterface& db,
                           const std::optional<ycsbr::BulkLoadWorkload>& load,
                           const std::optional<ycsbr::Workload>& workload) {
  ycsbr::BenchmarkOptions boptions;
  boptions.num_threads = FLAGS_threads;
  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << boptions.num_threads
              << " application thread(s)." << std::endl;
  }

  if (load.has_value() && workload.has_value()) {
    // Load then run the workload.
    return ycsbr::RunTimedWorkload(db, *load, *workload, boptions);

  } else if (workload.has_value()) {
    // Workload only (database assumed to be preloaded).
    return ycsbr::RunTimedWorkload(db, *workload, boptions);

  } else if (load.has_value()) {
    // Bulk load only.
    return ycsbr::RunTimedWorkload(db, *load);

  } else {
    // Nothing to run.
    return ycsbr::BenchmarkResult(std::chrono::nanoseconds(0));
  }
}

void PrintExperimentResult(const std::string& db,
                           ycsbr::BenchmarkResult result) {
  std::cout << db << ",";
  result.PrintAsCSV(std::cout, /*print_header=*/false);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run YCSB workloads on LLSM and RocksDB.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  DBType db = llsm::bench::ParseDBType(FLAGS_db).value();
  RocksDBInterface rocksdb;
  LLSMInterface llsm;

  std::optional<ycsbr::BulkLoadWorkload> load;
  if (!FLAGS_load_path.empty()) {
    ycsbr::Workload::Options loptions;
    loptions.value_size = FLAGS_record_size_bytes - 8;
    load = ycsbr::BulkLoadWorkload::LoadFromFile(FLAGS_load_path, loptions);

    // The workload load function maintains keys in lexicographic order (though
    // the keys themselves are 64-bit integers). LLSM needs these hints as
    // 64-bit integers.
    auto minmax = load->GetKeyRange();
    llsm.SetKeyDistHints(/*min_key=*/__builtin_bswap64(minmax.min),
                         /*max_key=*/__builtin_bswap64(minmax.max),
                         /*num_keys=*/load->size());
  }

  std::optional<ycsbr::Workload> workload;
  if (!FLAGS_workload_path.empty()) {
    ycsbr::Workload::Options options;
    options.value_size = FLAGS_record_size_bytes - 8;
    workload = ycsbr::Workload::LoadFromFile(FLAGS_workload_path, options);
  }

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (db == DBType::kAll || db == DBType::kRocksDB) {
    PrintExperimentResult("rocksdb", Run(rocksdb, load, workload));
  }
  if (db == DBType::kAll || db == DBType::kLLSM) {
    PrintExperimentResult("llsm", Run(llsm, load, workload));
  }

  return 0;
}
