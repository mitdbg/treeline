#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "bench/common/config.h"
#include "bench/common/data.h"
#include "bench/common/timing.h"
#include "gflags/gflags.h"
#include "llsm/db.h"
#include "llsm/options.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

namespace {

namespace fs = std::filesystem;
using llsm::bench::DBType;

DEFINE_uint64(data_mib, 64, "The amount of user data to write, in MiB.");
DEFINE_uint64(write_batch_size, 1024,
              "The size of a write batch used by RocksDB.");
DEFINE_bool(shuffle, false, "Whether or not to shuffle the generated dataset.");

std::chrono::nanoseconds RunRocksDBExperiment(
    const llsm::bench::U64Dataset& dataset) {
  rocksdb::DB* db = nullptr;
  rocksdb::Options options = llsm::bench::BuildRocksDBOptions();
  options.PrepareForBulkLoad();
  options.create_if_missing = true;
  options.error_if_exists = true;

  const std::string dbname = FLAGS_db_path + "/rocksdb";
  rocksdb::Status status = rocksdb::DB::Open(options, dbname, &db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
  }

  return llsm::bench::MeasureRunTime([db, &dataset]() {
    rocksdb::WriteOptions woptions;
    woptions.disableWAL = FLAGS_bypass_wal;
    rocksdb::Status status;
    rocksdb::WriteBatch batch;
    for (const auto& record : dataset) {
      status = batch.Put(
          rocksdb::Slice(record.key().data(), record.key().size()),
          rocksdb::Slice(record.value().data(), record.value().size()));
      if (!status.ok()) {
        throw std::runtime_error("Failed to write record to RocksDB instance.");
      }
      if (batch.Count() >= FLAGS_write_batch_size) {
        status = db->Write(woptions, &batch);
        if (!status.ok()) {
          throw std::runtime_error("Failed to write batch to RocksDB.");
        }
        batch.Clear();
      }
    }
    if (batch.Count() > 0) {
      status = db->Write(woptions, &batch);
      if (!status.ok()) {
        throw std::runtime_error("Failed to write batch to RocksDB.");
      }
    }
    // Ensure any remaining in-memory data is flushed to disk
    const rocksdb::FlushOptions foptions;
    status = db->Flush(foptions);
    if (!status.ok()) {
      throw std::runtime_error("Failed to flush memtable at the end.");
    }
    // Compact the entire key range
    rocksdb::CompactRangeOptions coptions;
    coptions.change_level = true;
    status = db->CompactRange(coptions, nullptr, nullptr);
    if (!status.ok()) {
      throw std::runtime_error("Failed to compact at the end.");
    }
    delete db;
  });
}

std::chrono::nanoseconds RunLLSMExperiment(
    const llsm::bench::U64Dataset& dataset) {
  llsm::DB* db = nullptr;
  llsm::Options options = llsm::bench::BuildLLSMOptions();
  options.key_hints.num_keys = dataset.size();

  const std::string dbname = FLAGS_db_path + "/llsm";
  llsm::Status status = llsm::DB::Open(options, dbname, &db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open LLSM: " + status.ToString());
  }

  return llsm::bench::MeasureRunTime([db, &dataset]() {
    llsm::WriteOptions woptions;
    if (!FLAGS_shuffle) {
      woptions.sorted_load = true;
      woptions.perform_checks = false;
    }
    woptions.bypass_wal = FLAGS_bypass_wal;
    llsm::Status status;
    for (const auto& record : dataset) {
      status = db->Put(woptions, record.key(), record.value());
      if (!status.ok()) {
        throw std::runtime_error("Failed to write record to LLSM.");
      }
    }
    delete db;
  });
}

void PrintExperimentResult(const std::string& db,
                           const llsm::bench::U64Dataset& dataset,
                           std::chrono::nanoseconds run_time) {
  const std::chrono::duration<double> run_time_s = run_time;
  const double throughput_mib_per_s = FLAGS_data_mib / run_time_s.count();
  const double throughput_mops_per_s =
      dataset.size() / run_time_s.count() / 1e6;
  std::cout << db << "," << FLAGS_data_mib << "," << FLAGS_bg_threads << ","
            << FLAGS_record_size_bytes << "," << throughput_mib_per_s << ","
            << throughput_mops_per_s << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Measure the write throughput when writing a fixed amount of synthetic "
      "data into an empty database.");
  gflags::ParseCommandLineFlags(&argc, &argv, /* remove_flags */ true);
  if (FLAGS_data_mib == 0) {
    std::cerr << "ERROR: --data_mib must be greater than 0." << std::endl;
    return 1;
  }
  if (fs::exists(FLAGS_db_path)) {
    std::cerr << "ERROR: The provided --db_path already exists." << std::endl;
    return 1;
  }
  DBType db = llsm::bench::ParseDBType(FLAGS_db).value();

  llsm::bench::U64Dataset::GenerateOptions dataset_options;
  dataset_options.record_size = FLAGS_record_size_bytes;
  dataset_options.shuffle = FLAGS_shuffle;
  dataset_options.rng_seed = FLAGS_seed;
  const llsm::bench::U64Dataset dataset =
      llsm::bench::U64Dataset::Generate(FLAGS_data_mib, dataset_options);

  std::cout << "db,data_size_mib,bg_threads,record_size_bytes,throughput_mib_"
               "per_s,throughput_mops_per_s"
            << std::endl;

  for (uint32_t i = 0; i < FLAGS_trials; ++i) {
    fs::create_directory(FLAGS_db_path);

    if (db == DBType::kAll || db == DBType::kRocksDB) {
      PrintExperimentResult("rocksdb", dataset, RunRocksDBExperiment(dataset));
    }
    if (db == DBType::kAll || db == DBType::kLLSM) {
      PrintExperimentResult("llsm", dataset, RunLLSMExperiment(dataset));
    }

    fs::remove_all(FLAGS_db_path);
  }

  return 0;
}
