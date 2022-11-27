#include <map>

#include "benchmark/benchmark.h"
#include "bufmgr/buffer_manager.h"
#include "common/config.h"
#include "db/page.h"
#include "gflags/gflags.h"
#include "treeline/options.h"
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

void SimulateBM(benchmark::State& state, bool large_buffer) {
  // Obtain and process the bulk load workload.
  size_t record_size = state.range(0);
  ycsbr::Trace::Options loptions;
  loptions.use_v1_semantics = true;
  loptions.value_size = record_size - 8;
  loptions.sort_requests = true;
  loptions.swap_key_bytes = false;
  ycsbr::BulkLoadTrace load =
      ycsbr::BulkLoadTrace::LoadFromFile(FLAGS_load_path, loptions);

  // Create key hints.
  tl::KeyDistHints key_hints;
  key_hints.num_keys = load.size();
  key_hints.page_fill_pct = FLAGS_tl_page_fill_pct;
  key_hints.record_size = record_size;

  // Initialize an alex instance.
  std::map<uint64_t, tl::PhysicalPageId> model;
  size_t records_per_page = key_hints.records_per_page();
  size_t i = 0;
  for (const auto& req : load) {
    if (i % records_per_page == 0) {
      tl::PhysicalPageId page_id(0, i / records_per_page);
      model.insert({__builtin_bswap64(req.key), page_id});
    }
    ++i;
  }

  // Open workload.
  ycsbr::Trace::Options woptions;
  woptions.use_v1_semantics = true;
  woptions.value_size = record_size - 8;
  woptions.swap_key_bytes = false;
  ycsbr::Trace workload =
      ycsbr::Trace::LoadFromFile(FLAGS_workload_path, woptions);

  // Pre-calculate page numbers appropriately
  std::vector<tl::PhysicalPageId> page_ids;
  for (const auto& req : workload) {
    auto it = model.upper_bound(__builtin_bswap64(req.key));
    --it;
    page_ids.push_back(it->second);
  }

  // Create buffer manager options.
  tl::BufMgrOptions bm_options;
  bm_options.simulation_mode = true;
  bm_options.buffer_pool_size =
      large_buffer ? model.size() * tl::Page::kSize : 64 * 1024 * 1024;

  // Bookkeeping
  std::string db_path = "test";
  double numIOs = 0;

  for (auto _ : state) {
    tl::BufferManager buf_mgr(bm_options, db_path);
    for (const auto& page_id : page_ids) {
      auto& bf = buf_mgr.FixPage(page_id, /*exclusive = */ true);
      if (bf.IsNewlyFixed()) ++numIOs;
      buf_mgr.UnfixPage(bf, /*is_dirty = */ true);
    }
  }

  state.SetBytesProcessed(state.iterations() * workload.size() * record_size);
  state.counters["IOs"] = numIOs / state.iterations();
}

BENCHMARK_CAPTURE(SimulateBM, no_large_buffer, /*large_buffer = */ false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(SimulateBM, large_buffer, /*large_buffer = */ true)
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
