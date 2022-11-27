// Benchmarks that measure the `ThreadPool`'s submit overhead.
//
// Build the benchmarks by enabling the `TL_BUILD_BENCHMARKS` option when
// configuring the project. Then run the `microbench` executable under `bench`.
//
//   mkdir build && cd build
//   cmake -DCMAKE_BUILD_TYPE=Release -DTL_BUILD_BENCHMARKS=ON ..
//   make -j
//   ./bench/microbench --benchmark_filter=ThreadPool*
//
// Example run:
// 2021-01-19T15:50:59-05:00
// Running ./bench/microbench
// Run on (40 X 3900 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x20)
//   L1 Instruction 32 KiB (x20)
//   L2 Unified 1024 KiB (x20)
//   L3 Unified 28160 KiB (x1)
// Load Average: 0.31, 0.51, 0.54
// -------------------------------------------------------------------------------
// Benchmark                                     Time             CPU   Iterations
// -------------------------------------------------------------------------------
// BM_ThreadPoolSubmitOverhead/1               347 ns          339 ns      1617071
// BM_ThreadPoolSubmitOverhead/2               914 ns          836 ns       915675
// BM_ThreadPoolSubmitOverhead/4              4104 ns         3256 ns       265528
// BM_ThreadPoolSubmitOverhead/8              3385 ns         2896 ns       276004
// BM_ThreadPoolSubmitOverhead/16             2703 ns         2507 ns       292201
// BM_ThreadPoolSubmitNoWaitOverhead/1         226 ns          219 ns      3167486
// BM_ThreadPoolSubmitNoWaitOverhead/2         308 ns          286 ns      2396332
// BM_ThreadPoolSubmitNoWaitOverhead/4        3695 ns         2868 ns       257309
// BM_ThreadPoolSubmitNoWaitOverhead/8        2690 ns         2328 ns       319681
// BM_ThreadPoolSubmitNoWaitOverhead/16       2504 ns         2254 ns       345251

#include <future>
#include <vector>

#include "benchmark/benchmark.h"
#include "util/thread_pool.h"

namespace {

void NoOp() {}

void BM_ThreadPoolSubmitOverhead(benchmark::State& state) {
  tl::ThreadPool pool(state.range(0));
  std::vector<std::future<void>> futures;
  // Avoid measuring vector resizing
  futures.reserve(5000000);

  // Measure the time it takes to submit work
  for (auto _ : state) {
    futures.emplace_back(pool.Submit(NoOp));
  }

  for (auto& f : futures) {
    f.get();
  }
}

void BM_ThreadPoolSubmitNoWaitOverhead(benchmark::State& state) {
  tl::ThreadPool pool(state.range(0));
  for (auto _ : state) {
    pool.SubmitNoWait(NoOp);
  }
}

BENCHMARK(BM_ThreadPoolSubmitOverhead)
    ->RangeMultiplier(2)
    ->Range(1, 16);  // Number of threads to use

BENCHMARK(BM_ThreadPoolSubmitNoWaitOverhead)
    ->RangeMultiplier(2)
    ->Range(1, 16);  // Number of threads to use

}  // namespace
