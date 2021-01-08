#include <stdexcept>

#include "benchmark/benchmark.h"
#include "bench/common/data.h"
#include "db/memtable.h"

namespace {

using namespace llsm;

void MemTableInsert_64MiB(benchmark::State& state, bool shuffle) {
  constexpr size_t kDatasetSizeMiB = 64;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  options.shuffle = shuffle;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);
  Status s;
  for (auto _ : state) {
    MemTable mtable;
    for (const auto& record : dataset) {
      s = mtable.Put(record.key(), record.value());
      if (!s.ok()) {
        throw std::runtime_error("Failed to insert record into the memtable!");
      }
    }
  }
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);
}

BENCHMARK_CAPTURE(MemTableInsert_64MiB, in_order, /*shuffle=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(MemTableInsert_64MiB, shuffled, /*shuffle=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

}  // namespace
