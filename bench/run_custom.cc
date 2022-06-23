#include <filesystem>
#include <iostream>
#include <string_view>

#include "bench/common/config.h"
#include "bench/common/leanstore_interface.h"
#include "bench/common/load_data.h"
#include "bench/common/pg_treeline_interface.h"
#include "bench/common/rocksdb_interface.h"
#include "bench/common/startup.h"
#include "bench/common/treeline_interface.h"
#include "gflags/gflags.h"
#include "ycsbr/gen.h"

namespace {

namespace fs = std::filesystem;
using namespace tl::bench;

DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_string(workload_config, "",
              "The path to the workload configuration file");
DEFINE_bool(skip_workload, false,
            "If set to true, this workload runner will skip running the "
            "workload (it will only run the load portion of the workload).");
DEFINE_string(custom_dataset, "", "A path to a custom dataset.");

DEFINE_string(output_path, "",
              "A path to where additional output should be written (e.g., "
              "throughput samples, statistics).");
DEFINE_uint64(throughput_sample_period, 0,
              "How frequently to sample the achieved throughput. Set to 0 to "
              "disable sampling.");
DEFINE_bool(notify_after_init, false,
            "If set to true, this process will send a SIGUSR1 signal to its "
            "parent process after database initialization completes.");

DEFINE_string(
    custom_inserts, "",
    "Use this flag to specify custom insert files for use in the workload. The "
    "names should correspond to names in the workload configuration file. "
    "Expected format: <name>:<path>. To specify more than one custom insert "
    "list, separate the entries using a comma.");

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

  if (FLAGS_notify_after_init) {
    SendReadySignalToParent();
  }
  if (FLAGS_skip_workload) {
    std::cerr << "> Skipping the workload." << std::endl;
    return ycsbr::BenchmarkResult(std::chrono::nanoseconds(0));
  }
  if (FLAGS_verbose) {
    std::cerr << "> Running workload using " << FLAGS_threads
              << " application thread(s)." << std::endl;
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  options.throughput_sample_period = FLAGS_throughput_sample_period;
  options.output_dir = std::filesystem::path(FLAGS_output_path);
  options.throughput_output_file_prefix = "throughput-";
  const auto result = session.RunWorkload(workload, options);
  session.Terminate();

  if (!FLAGS_output_path.empty()) {
    session.db().WriteOutStats(fs::path(FLAGS_output_path));
  }
  return result;
}

void PrintExperimentResult(const std::string& db,
                           ycsbr::BenchmarkResult result) {
  std::cout << db << ",";
  result.PrintAsCSV(std::cout, /*print_header=*/false);
}

void ProcessCustomInserts(
    std::unique_ptr<ycsbr::gen::PhasedWorkload>& workload) {
  if (FLAGS_custom_inserts.empty()) return;

  std::string_view remaining(FLAGS_custom_inserts);
  while (!remaining.empty()) {
    const auto pos = remaining.find(',');
    const auto entry = remaining.substr(0, pos);
    if (pos == std::string_view::npos) {
      remaining = std::string_view();
    } else {
      remaining = remaining.substr(pos + 1);
    }

    const auto sep_pos = entry.find(':');
    if (sep_pos == std::string_view::npos) {
      throw std::invalid_argument(
          "Invalid format for --custom_inserts (missing ':' separator).");
    }
    const std::string name = std::string(entry.substr(0, sep_pos));
    const std::string path = std::string(entry.substr(sep_pos + 1));
    std::vector<ycsbr::Request::Key> keys =
        LoadDatasetFromTextFile(path, /*warn_on_duplicates=*/FLAGS_verbose);
    if (keys.empty()) {
      throw std::invalid_argument("Dataset at " + path + " is empty.");
    }
    workload->AddCustomInsertList(name, keys);
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run generated workloads on TreeLine and RocksDB.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_workload_config.empty()) {
    std::cerr << "ERROR: Please provide a workload configuration file."
              << std::endl;
    return 1;
  }

  DBType db = tl::bench::ParseDBType(FLAGS_db).value();
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
  ProcessCustomInserts(workload);

  // The record size is specified in the workload configuration file. We need
  // to keep this command line option to support the other benchmark drivers
  // which use this option.
  FLAGS_record_size_bytes = workload->GetRecordSizeBytes();

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }
  if (!FLAGS_output_path.empty() && !fs::exists(FLAGS_output_path)) {
    fs::create_directory(FLAGS_output_path);
  }

  std::cout << "db,";
  ycsbr::BenchmarkResult::PrintCSVHeader(std::cout);

  if (db == DBType::kAll || db == DBType::kRocksDB) {
    PrintExperimentResult("rocksdb", Run<RocksDBInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kTreeLine) {
    // NOTE: We use the legacy `llsm` database name in the results for backward
    // compatibility with our experiment scripts and cached results.
    PrintExperimentResult("llsm", Run<TreeLineInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kLeanStore) {
    PrintExperimentResult("leanstore", Run<LeanStoreInterface>(*workload));
  }
  if (db == DBType::kAll || db == DBType::kPGTreeLine) {
    // NOTE: We use the legacy `pg_llsm` database name in the results for
    // backward compatibility with our experiment scripts and cached results.
    PrintExperimentResult("pg_llsm", Run<PGTreeLineInterface>(*workload));
  }

  return 0;
}
