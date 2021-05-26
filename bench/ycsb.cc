#include <filesystem>
#include <iostream>
#include <optional>
#include <string>

#include "bench/common/config.h"
#include "bench/common/kvell_interface.h"
#include "bench/common/leanstore_interface.h"
#include "bench/common/llsm_interface.h"
#include "bench/common/rocksdb_interface.h"
#include "gflags/gflags.h"
#include "ycsbr/ycsbr.h"

namespace {

namespace fs = std::filesystem;
using llsm::bench::DBType;

DEFINE_string(load_path, "", "Path to the bulk load workload file, if needed.");
DEFINE_string(workload_path, "", "Path to the workload file.");
DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(const std::optional<ycsbr::BulkLoadTrace>& load,
                           const std::optional<ycsbr::Trace>& workload) {
  ycsbr::Session<DatabaseInterface> session(FLAGS_threads);
  if (load.has_value()) {
    auto minmax = load->GetKeyRange();
    session.db().SetKeyDistHints(/*min_key=*/minmax.min,
                                 /*max_key=*/minmax.max,
                                 /*num_keys=*/load->size());
  }
  session.Initialize();

  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << FLAGS_threads
              << " application thread(s)." << std::endl;
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  // Currently there are no negative lookups - all requests should succeed
  // (otherwise something is wrong in the implementation).
  options.expect_request_success = true;

  if (load.has_value() && workload.has_value()) {
    // Load then run the workload.
    session.ReplayBulkLoadTrace(*load);
    return session.ReplayTrace(*workload, options);

  } else if (workload.has_value()) {
    // Workload only (database assumed to be preloaded).
    return session.ReplayTrace(*workload, options);

  } else if (load.has_value()) {
    // Bulk load only.
    return session.ReplayBulkLoadTrace(*load);

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

  std::optional<ycsbr::BulkLoadTrace> load;
  if (!FLAGS_load_path.empty()) {
    ycsbr::Trace::Options loptions;
    loptions.value_size = FLAGS_record_size_bytes - 8;
    load = ycsbr::BulkLoadTrace::LoadFromFile(FLAGS_load_path, loptions);
  }

  std::optional<ycsbr::Trace> workload;
  if (!FLAGS_workload_path.empty()) {
    ycsbr::Trace::Options options;
    options.value_size = FLAGS_record_size_bytes - 8;
    workload = ycsbr::Trace::LoadFromFile(FLAGS_workload_path, options);
  }

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (db == DBType::kAll || db == DBType::kRocksDB) {
    PrintExperimentResult("rocksdb", Run<RocksDBInterface>(load, workload));
  }
  if (db == DBType::kAll || db == DBType::kLLSM) {
    PrintExperimentResult("llsm", Run<LLSMInterface>(load, workload));
  }
  if (db == DBType::kAll || db == DBType::kLeanStore) {
    PrintExperimentResult("leanstore", Run<LeanStoreInterface>(load, workload));
  }
  if (db == DBType::kAll || db == DBType::kKVell) {
    PrintExperimentResult("kvell", Run<KVellInterface>(load, workload));
  }

  return 0;
}
