#include <algorithm>
#include <numeric>
#include <random>
#include <stdexcept>

#include "bench/common/data.h"
#include "benchmark/benchmark.h"
#include "record_cache/record_cache.h"

namespace {

using namespace llsm;

void RecordCacheInsert_64MiB(benchmark::State& state, double fraction_cacheable,
                             double fraction_lookups) {
  constexpr size_t kDatasetSizeMiB = 64;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);
  Status s;
  const uint64_t num_records = dataset.size();
  const uint64_t cache_entries = num_records * fraction_cacheable;

  // Create an array to handle insert/lookup distinction
  const uint64_t threshold = dataset.size() * fraction_lookups;
  std::vector<int> v(num_records);
  std::iota(std::begin(v), std::end(v), 0);
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(v), std::end(v), rng);

  for (auto _ : state) {
    RecordCache rc(cache_entries);
    uint64_t i = 0;
    uint64_t index_out;
    for (const auto& record : dataset) {
      if (v[i++] < threshold) {
        s = rc.GetIndex(record.key(), &index_out);
        if (!s.IsNotFound()) {
          throw std::runtime_error("Found a record that shouldn't exist!");
        }
      } else {
        s = rc.Put(record.key(), record.value());
        if (!s.ok()) {
          throw std::runtime_error(
              "Failed to insert record into the record cache!");
        }
      }
    }
  }
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);
  state.SetItemsProcessed(state.iterations() * num_records);
}

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, one_tenth_100W,
                  /*fraction cacheable = */ 0.1, /*fraction_lookups = */ 0)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, full_100W,
                  /*fraction cacheable = */ 1,
                  /*fraction_lookups = */ 0)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, one_tenth_50W_50R,
                  /*fraction cacheable = */ 0.1, /*fraction_lookups = */ 0.5)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, full_50W_50R,
                  /*fraction cacheable = */ 1,
                  /*fraction_lookups = */ 0.5)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, one_tenth_100R,
                  /*fraction cacheable = */ 0.1, /*fraction_lookups = */ 1)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheInsert_64MiB, full_100R,
                  /*fraction cacheable = */ 1,
                  /*fraction_lookups = */ 1)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond);

}  // namespace
