#include <filesystem>
#include <iostream>

#include "bench/common/config.h"
#include "bench/common/leanstore_interface.h"
#include "bench/common/llsm_interface.h"
#include "bench/common/load_data.h"
#include "bench/common/rocksdb_interface.h"
#include "bench/common/startup.h"
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
DEFINE_string(custom_dataset, "", "A path to a custom dataset.");

DEFINE_string(output_path, llsm::bench::GetDefaultOutputPath(),
              "A path to where throughput samples should be written.");
DEFINE_uint64(throughput_sample_period, 0,
              "How frequently to sample the achieved throughput. Set to 0 to "
              "disable sampling.");
DEFINE_bool(notify_after_init, false,
            "If set to true, this process will send a SIGUSR1 signal to its "
            "parent process after database initialization completes.");

template <class DatabaseInterface>
ycsbr::BenchmarkResult Run(const ycsbr::gen::PhasedWorkload& workload) {
  ycsbr::Session<DatabaseInterface> session(FLAGS_threads);
  if (!FLAGS_skip_load) {
    auto load = workload.GetLoadTrace(/*sort_requests=*/true);
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

  if (FLAGS_notify_after_init) {
    SendReadySignalToParent();
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  options.throughput_sample_period = FLAGS_throughput_sample_period;
  options.output_dir = std::filesystem::path(FLAGS_output_path);
  options.throughput_output_file_prefix = "throughput-";
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
  if (FLAGS_throughput_sample_period > 0 && !fs::exists(FLAGS_output_path)) {
    // Only create the output directory if we will take throughput samples.
    fs::create_directory(FLAGS_output_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (db == DBType::kAll || db == DBType::kRocksDB) {
    PrintExperimentResult("rocksdb", Run<RocksDBInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kLLSM) {
    PrintExperimentResult("llsm", Run<LLSMInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kLeanStore) {
    PrintExperimentResult("leanstore", Run<LeanStoreInterface>(*workload));
  }

  return 0;
}
