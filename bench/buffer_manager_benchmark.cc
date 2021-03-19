#include "benchmark/benchmark.h"
#include "bufmgr/buffer_manager.h"
#include "common/config.h"
#include "db/page.h"
#include "gflags/gflags.h"
#include "llsm/options.h"
#include "model/direct_model.h"
#include "rs/builder.h"
#include "ycsbr/ycsbr.h"

namespace {

bool ValidateLoad(const char* flagname, const std::string& path) {
  if (!path.empty()) return true;
  std::cerr << "ERROR: --load_path cannot be empty." << std::endl;
  return false;
}

bool ValidateWorkload(const char* flagname, const std::string& path) {
  if (!path.empty()) return true;
  std::cerr << "ERROR: --workload_path cannot be empty." << std::endl;
  return false;
}

DEFINE_string(load_path, "", "Path to the bulk load workload file.");
DEFINE_validator(load_path, &ValidateLoad);

DEFINE_string(workload_path, "", "Path to the workload file.");
DEFINE_validator(workload_path, &ValidateWorkload);

void SimulateBM(benchmark::State& state, bool large_buffer,
                bool use_rs_not_direct) {
  // Obtain and process the bulk load workload.
  size_t record_size = state.range(0);
  ycsbr::Workload::Options loptions;
  loptions.value_size = record_size - 8;
  loptions.sort_requests = true;
  loptions.swap_key_bytes = false;
  ycsbr::BulkLoadWorkload load =
      ycsbr::BulkLoadWorkload::LoadFromFile(FLAGS_load_path, loptions);

  // Create key hints.
  llsm::KeyDistHints key_hints;
  key_hints.num_keys = load.size();
  key_hints.page_fill_pct = FLAGS_llsm_page_fill_pct;
  key_hints.record_size = record_size;

  // Initialize a RadixSpline.
  auto minmax = load.GetKeyRange();
  rs::Builder<uint64_t> rsb(__builtin_bswap64(minmax.min),
                            __builtin_bswap64(minmax.max));
  for (const auto& req : load) rsb.AddKey(__builtin_bswap64(req.key));
  const rs::RadixSpline<uint64_t> rs = rsb.Finalize();

  // Initialize a direct model.
  std::unordered_map<uint64_t, uint64_t> rank_map;
  size_t i = 0;
  for (const auto& req : load) rank_map.insert({req.key, i++});
  llsm::DirectModel dm(key_hints);

  // Open workload.
  ycsbr::Workload::Options woptions;
  woptions.value_size = record_size - 8;
  woptions.swap_key_bytes = false;
  ycsbr::Workload workload =
      ycsbr::Workload::LoadFromFile(FLAGS_workload_path, woptions);

  // Pre-calculate page numbers appropriately
  std::vector<size_t> page_ids;
  for (const auto& req : workload) {
    size_t page_id;

    if (use_rs_not_direct) {
      const size_t estimate =
          rs.GetEstimatedPosition(__builtin_bswap64(req.key));
      page_id = estimate / key_hints.records_per_page();
    } else {
      const size_t normalized_key = rank_map.at(req.key);
      page_id = dm.KeyToPageId(normalized_key);
    }

    page_ids.push_back(page_id);
  }

  // Create buffer manager options.
  llsm::BufMgrOptions bm_options;
  bm_options.SetNumPagesUsing(key_hints);
  bm_options.simulation_mode = true;
  bm_options.buffer_pool_size = large_buffer
                                    ? bm_options.num_pages * llsm::Page::kSize
                                    : 64 * 1024 * 1024;

  // Bookkeeping
  std::string db_path = "test";
  double numIOs = 0;

  for (auto _ : state) {
    llsm::BufferManager buf_mgr(bm_options, db_path);
    for (const auto& page_id : page_ids) {
      auto& bf = buf_mgr.FixPage(page_id, /*exclusive = */ true);
      if (bf.IsNewlyFixed()) ++numIOs;
      buf_mgr.UnfixPage(bf, /*is_dirty = */ true);
    }
  }

  state.SetBytesProcessed(state.iterations() * workload.size() * record_size);
  state.counters["IOs"] = numIOs / state.iterations();
}

BENCHMARK_CAPTURE(SimulateBM, no_large_buffer_direct, /*large_buffer = */ false,
                  /*use_rs_not_direct = */ false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(SimulateBM, large_buffer_direct, /*large_buffer = */ true,
                  /*use_rs_not_direct = */ false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(SimulateBM, no_large_buffer_rs, /*large_buffer = */ false,
                  /*use_rs_not_direct = */ true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(SimulateBM, large_buffer_rs, /*large_buffer = */ true,
                  /*use_rs_not_direct = */ true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Determine the latency of the buffer manager.");
  benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
