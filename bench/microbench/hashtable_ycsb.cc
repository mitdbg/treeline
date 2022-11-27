#include <filesystem>
#include <iostream>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "bufmgr/sync_hash_table.h"
#include "gflags/gflags.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "ycsbr/ycsbr.h"

namespace {

namespace fs = std::filesystem;

enum class HTType : uint32_t {
  kAll = 0,
  kUnorderedMap = 1,
  kSyncHashTable = 2,
  kCuckooHashMap = 3
};

std::optional<HTType> ParseHTType(const std::string& candidate) {
  static std::unordered_map<std::string, HTType> kStringToHTType = {
      {"all", HTType::kAll},
      {"unordered_map", HTType::kUnorderedMap},
      {"sync_hash_table", HTType::kSyncHashTable},
      {"libcuckoo", HTType::kCuckooHashMap}};

  auto it = kStringToHTType.find(candidate);
  if (it == kStringToHTType.end()) {
    return std::optional<HTType>();
  }
  return it->second;
}

bool ValidateHT(const char* flagname, const std::string& ht) {
  if (::ParseHTType(ht).has_value()) return true;
  std::cerr << "ERROR: Unknown hash table type: " << ht << std::endl;
  return false;
}

bool ValidateRecordSize(const char* flagname, uint32_t record_size) {
  if (record_size >= 9) return true;
  std::cerr << "ERROR: --record_size_bytes must be at least 9 (8 byte key + 1 "
               "byte value)."
            << std::endl;
  return false;
}

DEFINE_string(ht, "all",
              "Which hash table(s) to use {all, unordered_map, "
              "sync_hash_table, libcuckoo}.");
DEFINE_validator(ht, &ValidateHT);

DEFINE_uint32(record_size_bytes, 16, "The size of each record, in bytes.");
DEFINE_validator(record_size_bytes, &ValidateRecordSize);

DEFINE_string(load_path, "", "Path to the bulk load workload file, if needed.");
DEFINE_string(workload_path, "", "Path to the workload file.");
DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_bool(verbose, false,
            "If set, benchmark information will be printed to stderr.");
DEFINE_uint32(sht_partitions, 16,
              "The number of partitions to use in the sync hash table");

class UnorderedMapInterface {
 public:
  UnorderedMapInterface() {}
  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  // Called once before the benchmark.
  void InitializeDatabase() {}

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {}

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& records) {
    map_.reserve(records.size());
    mutex_.lock();
    for (const auto& rec : records) {
      std::string key_string(reinterpret_cast<const char*>(&rec.key),
                             sizeof(rec.key));
      std::string value_string(rec.value, rec.value_size);
      map_.insert({key_string, value_string});
    }
    mutex_.unlock();
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    mutex_.lock_shared();
    auto search = map_.find(key_string);
    mutex_.unlock_shared();
    if (search != map_.end()) {
      value_out = &(search->second);
      return true;
    } else {
      return false;
    }
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    std::string value_string(value, value_size);
    mutex_.lock();
    map_.insert_or_assign(key_string, value_string);
    mutex_.unlock();
    return true;
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
  std::unordered_map<std::string, std::string> map_;
  std::shared_mutex mutex_;
};

class SyncHashTableInterface {
 public:
  SyncHashTableInterface() : map_(nullptr) {}
  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  // Called once before the benchmark.
  void InitializeDatabase() {}

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {}

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& records) {
    map_ = std::make_unique<tl::SyncHashTable<std::string, std::string>>(
        records.size(), FLAGS_sht_partitions);
    for (const auto& rec : records) Insert(rec.key, rec.value, rec.value_size);
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    return map_->SafeLookup(key_string, value_out);
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    std::string value_string(value, value_size);

    return map_->SafeInsert(key_string, value_string);
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
  std::unique_ptr<tl::SyncHashTable<std::string, std::string>> map_;
};

class CuckooHashMapInterface {
 public:
  CuckooHashMapInterface() : map_(nullptr) {}
  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  // Called once before the benchmark.
  void InitializeDatabase() {}

  // Called once after the workload if `InitializeDatabase()` has been called.
  void ShutdownDatabase() {}

  // Load the records into the database.
  void BulkLoad(const ycsbr::BulkLoadTrace& records) {
    map_ =
        std::make_unique<libcuckoo::cuckoohash_map<std::string, std::string>>(
            records.size());
    for (const auto& rec : records) Insert(rec.key, rec.value, rec.value_size);
  }

  // Read the value at the specified key. Return true if the read succeeded.
  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    return map_->find(key_string, *value_out);
  }

  // Insert the specified key value pair. Return true if the insert succeeded.
  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    std::string key_string(reinterpret_cast<char*>(&key), sizeof(key));
    std::string value_string(value, value_size);

    map_->insert_or_assign(key_string, value_string);
    return true;
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
  std::unique_ptr<libcuckoo::cuckoohash_map<std::string, std::string>> map_;
};

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(const std::optional<ycsbr::BulkLoadTrace>& load,
                           const ycsbr::Trace& workload) {
  ycsbr::BenchmarkOptions<DatabaseInterface> boptions;
  boptions.num_threads = FLAGS_threads;
  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << boptions.num_threads
              << " application thread(s)." << std::endl;
  }

  if (load.has_value()) {
    return ycsbr::ReplayTrace(workload, &(*load), boptions);
  } else {
    return ycsbr::ReplayTrace(workload, nullptr, boptions);
  }
}

void PrintExperimentResult(const std::string& ht,
                           ycsbr::BenchmarkResult result) {
  std::cout << ht << ",";
  result.PrintAsCSV(std::cout, /*print_header=*/false);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run YCSB workloads on different hash tables.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_workload_path.empty()) {
    std::cerr << "ERROR: Please provide a workload." << std::endl;
    return 1;
  }
  HTType ht = ParseHTType(FLAGS_ht).value();

  std::optional<ycsbr::BulkLoadTrace> load;
  if (!FLAGS_load_path.empty()) {
    ycsbr::Trace::Options loptions;
    loptions.use_v1_semantics = true;
    loptions.value_size = FLAGS_record_size_bytes - 8;
    load = ycsbr::BulkLoadTrace::LoadFromFile(FLAGS_load_path, loptions);
  }

  ycsbr::Trace::Options options;
  options.use_v1_semantics = true;
  options.value_size = FLAGS_record_size_bytes - 8;
  ycsbr::Trace workload =
      ycsbr::Trace::LoadFromFile(FLAGS_workload_path, options);

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (ht == HTType::kAll || ht == HTType::kUnorderedMap) {
    PrintExperimentResult("unordered_map",
                          Run<UnorderedMapInterface>(load, workload));
  }
  if (ht == HTType::kAll || ht == HTType::kSyncHashTable) {
    PrintExperimentResult("sync_hash_table",
                          Run<SyncHashTableInterface>(load, workload));
  }
  if (ht == HTType::kAll || ht == HTType::kCuckooHashMap) {
    PrintExperimentResult("cuckoohash_map",
                          Run<CuckooHashMapInterface>(load, workload));
  }

  return 0;
}
