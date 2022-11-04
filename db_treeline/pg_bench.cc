#include <filesystem>
#include <fstream>
#include <iostream>

#include "bench/common/load_data.h"
#include "bench/common/startup.h"
#include "config.h"
#include "gflags/gflags.h"
#include "pg_interface.h"
#include "ycsbr/gen.h"

namespace {

namespace fs = std::filesystem;

DEFINE_string(output_path, ".",
              "A path to where the results should be written.");
DEFINE_uint32(threads, 1, "The number of threads to use to run the workload.");
DEFINE_string(workload_config, "",
              "The path to the workload configuration file");
DEFINE_bool(
    skip_load, false,
    "If set to true, this workload runner will skip the initial data load.");
DEFINE_string(custom_dataset, "", "A path to a custom dataset.");
DEFINE_uint32(record_size_bytes, 16, "The size of each record, in bytes.");

DEFINE_uint64(throughput_sample_period, 0,
              "How frequently to sample the achieved throughput. Set to 0 to "
              "disable sampling.");
DEFINE_uint32(latency_sample_period, 1,
              "The number of requests between latency measurements (i.e., "
              "measure latency every N-th request).");

DEFINE_bool(verbose, false,
            "If set, benchmark information will be printed to stderr.");
DEFINE_uint32(seed, 42,
              "The seed to use for the PRNG (to ensure reproducibility).");
DEFINE_bool(notify_after_init, false,
            "If set to true, this process will send a SIGUSR1 signal to its "
            "parent process after database initialization completes.");

ycsbr::BenchmarkResult Run(
    ycsbr::Session<tl::pg::PageGroupingInterface>& session,
    const ycsbr::gen::PhasedWorkload& workload) {
  if (!FLAGS_skip_load) {
    const auto load = workload.GetLoadTrace();
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
    tl::bench::SendReadySignalToParent();
  }

  ycsbr::RunOptions options;
  options.latency_sample_period = FLAGS_latency_sample_period;
  options.throughput_sample_period = FLAGS_throughput_sample_period;
  options.output_dir = std::filesystem::path(FLAGS_output_path);
  options.throughput_output_file_prefix = "throughput-";
  return session.RunWorkload(workload, options);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Run generated workloads against the page grouping prototype.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_workload_config.empty()) {
    std::cerr << "ERROR: Please provide a workload configuration file."
              << std::endl;
    return 1;
  }
  if (FLAGS_db_path.empty()) {
    std::cerr << "ERROR: Please provide a database path." << std::endl;
    return 1;
  }

  std::unique_ptr<ycsbr::gen::PhasedWorkload> workload =
      ycsbr::gen::PhasedWorkload::LoadFrom(FLAGS_workload_config, FLAGS_seed,
                                           FLAGS_record_size_bytes);

  if (!FLAGS_custom_dataset.empty()) {
    if (FLAGS_verbose) {
      std::cerr << "> Loading custom dataset: " << FLAGS_custom_dataset
                << std::endl;
    }
    std::vector<ycsbr::Request::Key> keys =
        tl::bench::LoadDatasetFromTextFile(
            FLAGS_custom_dataset, /*warn_on_duplicates=*/FLAGS_verbose);
    if (FLAGS_verbose) {
      std::cerr << "> Loaded a custom dataset with " << keys.size() << " keys."
                << std::endl;
    }
    workload->SetCustomLoadDataset(std::move(keys));
  }

  if (!fs::exists(FLAGS_db_path)) {
    fs::create_directory(FLAGS_db_path);
  }
  if (!fs::exists(FLAGS_output_path)) {
    fs::create_directory(FLAGS_output_path);
  }
  const fs::path output_dir = fs::path(FLAGS_output_path);

  // Run benchmark.
  ycsbr::Session<tl::pg::PageGroupingInterface> session(FLAGS_threads);
  const auto result = Run(session, *workload);
  if (FLAGS_verbose) {
    std::cerr << "> Done running workload." << std::endl;
  }
  session.Terminate();

  // Overall performance results.
  {
    std::ofstream overall(output_dir / "overall.csv");
    result.PrintAsCSV(overall, /*print_header=*/true);
  }

  // Read I/O statistics.
  {
    const auto& read_counts = session.db().GetReadCounts();
    std::ofstream out(output_dir / "read_counts.csv");
    out << "num_contiguous_pages,count" << std::endl;
    for (size_t i = 0; i < read_counts.size(); ++i) {
      out << (i + 1) << "," << read_counts[i] << std::endl;
    }
  }

  // Write I/O statistics.
  {
    const auto& write_counts = session.db().GetWriteCounts();
    std::ofstream out(output_dir / "write_counts.csv");
    out << "num_contiguous_pages,count" << std::endl;
    for (size_t i = 0; i < write_counts.size(); ++i) {
      out << (i + 1) << "," << write_counts[i] << std::endl;
    }
  }

  return 0;
}
