#include <filesystem>
#include <iostream>

#include "bench/common/config.h"
#include "bench/common/load_data.h"
#include "gflags/gflags.h"
#include "llsm/db.h"
#include "util/key.h"
#include "ycsbr/gen.h"

namespace {

namespace fs = std::filesystem;
using namespace llsm::bench;

DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_string(workload_config, "",
              "The path to the workload configuration file");
DEFINE_string(custom_dataset, "", "A path to a custom dataset.");
DEFINE_bool(verify_deletes, false,
            "If set, will verify that the deleted keys are not found.");

// Deletes all inserts in the workload.
class LLSMDeleteInsertsInterface {
 public:
  LLSMDeleteInsertsInterface() : db_(nullptr) {}

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/llsm";
    llsm::Options options = llsm::bench::BuildLLSMOptions();
    llsm::Status status = llsm::DB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start LLSM: " + status.ToString());
    }
  }

  void ShutdownDatabase() {
    if (db_ == nullptr) {
      return;
    }
    delete db_;
    db_ = nullptr;
  }

  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    throw std::runtime_error("This tool does not support bulk load.");
  }

  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return true;
  }

  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const llsm::key_utils::IntKeyAsSlice strkey(key);
    llsm::WriteOptions options;
    options.bypass_wal = FLAGS_bypass_wal;
    // We purposefully delete the inserted key.
    llsm::Status status = db_->Delete(options, strkey.as<llsm::Slice>());
    return status.ok();
  }

  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    value_out->clear();
    return true;
  }

  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    scan_out->clear();
    return true;
  }

 private:
  llsm::DB* db_;
};

// Verify the deletes.
class LLSMVerifyDeletesInterface {
 public:
  LLSMVerifyDeletesInterface() : db_(nullptr) {}

  const std::vector<ycsbr::Request::Key>& not_deleted_keys() const {
    return not_deleted_keys_;
  }

  void InitializeWorker(const std::thread::id& id) {}
  void ShutdownWorker(const std::thread::id& id) {}

  void InitializeDatabase() {
    const std::string dbname = FLAGS_db_path + "/llsm";
    llsm::Options options = llsm::bench::BuildLLSMOptions();
    llsm::Status status = llsm::DB::Open(options, dbname, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to start LLSM: " + status.ToString());
    }
  }

  void ShutdownDatabase() {
    if (db_ == nullptr) {
      return;
    }
    delete db_;
    db_ = nullptr;
  }

  void BulkLoad(const ycsbr::BulkLoadTrace& load) {
    throw std::runtime_error("This tool does not support bulk load.");
  }

  bool Update(ycsbr::Request::Key key, const char* value, size_t value_size) {
    return true;
  }

  bool Insert(ycsbr::Request::Key key, const char* value, size_t value_size) {
    const llsm::key_utils::IntKeyAsSlice strkey(key);
    static std::string out;
    llsm::Status status =
        db_->Get(llsm::ReadOptions(), strkey.as<llsm::Slice>(), &out);
    if (!status.IsNotFound()) {
      not_deleted_keys_.push_back(key);
    }
    return true;
  }

  bool Read(ycsbr::Request::Key key, std::string* value_out) {
    value_out->clear();
    return true;
  }

  bool Scan(
      const ycsbr::Request::Key key, const size_t amount,
      std::vector<std::pair<ycsbr::Request::Key, std::string>>* scan_out) {
    scan_out->clear();
    return true;
  }

 private:
  llsm::DB* db_;
  // This stores keys that should have been deleted but were actually found.
  std::vector<ycsbr::Request::Key> not_deleted_keys_;
};

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(const ycsbr::gen::PhasedWorkload& workload) {
  ycsbr::Session<DatabaseInterface> session(FLAGS_threads);
  session.Initialize();

  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << FLAGS_threads
              << " application thread(s)." << std::endl;
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  return session.RunWorkload(workload, options);
}

void VerifyDeletes(const ycsbr::gen::PhasedWorkload& workload) {
  ycsbr::Session<LLSMVerifyDeletesInterface> session(FLAGS_threads);
  session.Initialize();
  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  session.RunWorkload(workload, options);

  const auto& ndks = session.db().not_deleted_keys();
  if (ndks.empty()) {
    std::cerr << "> All inserts were deleted." << std::endl;
    return;
  }

  std::cerr << "> " << ndks.size()
            << " inserts that should have been deleted were still found."
            << std::endl;
}

void PrintExperimentResult(const std::string& db,
                           ycsbr::BenchmarkResult result) {
  std::cout << db << ",";
  result.PrintAsCSV(std::cout, /*print_header=*/false);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Deletes all inserted keys from an LLSM database.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_workload_config.empty()) {
    std::cerr << "ERROR: Please provide a workload configuration file."
              << std::endl;
    return 1;
  }

  DBType db = llsm::bench::ParseDBType(FLAGS_db).value();
  if (db != DBType::kLLSM) {
    std::cerr << "ERROR: This tool only supports the \"llsm\" database type."
              << std::endl;
    return 1;
  }

  std::unique_ptr<ycsbr::gen::PhasedWorkload> workload =
      ycsbr::gen::PhasedWorkload::LoadFrom(FLAGS_workload_config, FLAGS_seed,
                                           FLAGS_record_size_bytes);

  if (!FLAGS_custom_dataset.empty()) {
    std::vector<ycsbr::Request::Key> keys = LoadDatasetFromTextFile(
        FLAGS_custom_dataset, /*warn_on_duplicates=*/FLAGS_verbose);
    if (FLAGS_verbose) {
      std::cerr << "> Loaded a custom dataset with " << keys.size() << " keys."
                << std::endl;
    }
    workload->SetCustomLoadDataset(std::move(keys));
  }

  // The record size is specified in the workload configuration file. We need
  // to keep this command line option to support the other benchmark drivers
  // which use this option.
  FLAGS_record_size_bytes = workload->GetRecordSizeBytes();

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);
  PrintExperimentResult("llsm", Run<LLSMDeleteInsertsInterface>(*workload));

  if (FLAGS_verify_deletes) {
    VerifyDeletes(*workload);
  }

  return 0;
}
