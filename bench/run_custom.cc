#include <filesystem>
#include <iostream>

#include "bench/common/config.h"
#include "bench/common/llsm_interface.h"
#include "bench/common/rocksdb_interface.h"
#include "gflags/gflags.h"
#include "ycsbr/gen.h"

namespace {

namespace fs = std::filesystem;
using namespace llsm::bench;

DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_string(workload_config, "",
              "The path to the workload configuration file");
DEFINE_bool(
    skip_load, false,
    "If set to true, this workload runner will skip the initial data load.");

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(const ycsbr::gen::PhasedWorkload& workload) {
  ycsbr::Session<DatabaseInterface> session(FLAGS_threads);
  if (!FLAGS_skip_load) {
    auto load = workload.GetLoadTrace();
    auto minmax = load.GetKeyRange();
    session.db().SetKeyDistHints(/*min_key=*/minmax.min,
                                 /*max_key=*/minmax.max,
                                 /*num_keys=*/load.size());
    session.Initialize();
    if (FLAGS_verbose) {
      std::cerr << "> Loading " << load.size() << " records..." << std::endl;
    }
    session.ReplayBulkLoadTrace(load);
  } else {
    if (FLAGS_verbose) {
      std::cerr << "> Skipping the initial data load." << std::endl;
    }
    session.Initialize();
  }

  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << FLAGS_threads
              << " application thread(s)." << std::endl;
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  // Currently there are no negative lookups - all requests should succeed
  // (otherwise something is wrong in the implementation).
  options.expect_request_success = true;
  return session.RunWorkload(workload, options);
}

void PrintExperimentResult(const std::string& db,
                           ycsbr::BenchmarkResult result) {
  std::cout << db << ",";
  result.PrintAsCSV(std::cout, /*print_header=*/false);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run generated workloads on LLSM and RocksDB.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_workload_config.empty()) {
    std::cerr << "ERROR: Please provide a workload configuration file."
              << std::endl;
    return 1;
  }

  DBType db = llsm::bench::ParseDBType(FLAGS_db).value();
  std::unique_ptr<ycsbr::gen::PhasedWorkload> workload =
      ycsbr::gen::PhasedWorkload::LoadFrom(FLAGS_workload_config, FLAGS_seed);

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (db == DBType::kAll || db == DBType::kRocksDB) {
    PrintExperimentResult("rocksdb", Run<RocksDBInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kLLSM) {
    PrintExperimentResult("llsm", Run<LLSMInterface>(*workload));
  }

  return 0;
}
