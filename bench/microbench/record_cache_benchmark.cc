#include <algorithm>
#include <numeric>
#include <random>
#include <stdexcept>

#include "bench/common/data.h"
#include "benchmark/benchmark.h"
#include "record_cache/record_cache.h"

namespace {

using namespace tl;

void RecordCacheRW_64MiB(benchmark::State& state, bool is_safe) {
  constexpr size_t kDatasetSizeMiB = 64;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);
  Status s;
  const uint64_t num_records = dataset.size();
  const uint64_t cache_entries = num_records / state.range(1);

  // Create an array to handle insert/lookup distinction
  const uint64_t threshold =
      state.range(2) ? dataset.size() / state.range(3) : 0;
  std::vector<int> v(num_records);
  std::iota(std::begin(v), std::end(v), 0);
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(v), std::end(v), rng);

  for (auto _ : state) {
    RecordCache rc(cache_entries, state.range(4));
    uint64_t i = 0;
    uint64_t index_out;
    for (const auto& record : dataset) {
      if (v[i++] < threshold) {
        s = rc.GetCacheIndex(record.key(), /*exclusive = */ false, &index_out,
                             is_safe);
        if (s.ok() && is_safe) rc.cache_entries[index_out].Unlock();
      } else {
        s = rc.Put(record.key(), record.value(), /*is_dirty  = */ true,
                   tl::format::WriteType::kWrite,
                   RecordCache::kDefaultPriority, is_safe);
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

// Arguments are: {record_size, data:cache ratio, has_lookups(0/1),
// ops:lookups ratio, use_lru}
BENCHMARK_CAPTURE(RecordCacheRW_64MiB, unsafe, /*is_safe = */ false)
    ->Args({16, 10, 0, 0, 0})
    ->Args({512, 10, 0, 0, 0})
    ->Args({16, 1, 0, 0, 0})
    ->Args({512, 1, 0, 0, 0})
    ->Args({16, 10, 1, 2, 0})
    ->Args({512, 10, 1, 2, 0})
    ->Args({16, 1, 1, 2, 0})
    ->Args({512, 1, 1, 2, 0})
    ->Args({16, 10, 1, 1, 0})
    ->Args({512, 10, 1, 1, 0})
    ->Args({16, 1, 1, 1, 0})
    ->Args({512, 1, 1, 1, 0})
    ->Args({16, 10, 0, 0, 1})
    ->Args({512, 10, 0, 0, 1})
    ->Args({16, 1, 0, 0, 1})
    ->Args({512, 1, 0, 0, 1})
    ->Args({16, 10, 1, 2, 1})
    ->Args({512, 10, 1, 2, 1})
    ->Args({16, 1, 1, 2, 1})
    ->Args({512, 1, 1, 2, 1})
    ->Args({16, 10, 1, 1, 1})
    ->Args({512, 10, 1, 1, 1})
    ->Args({16, 1, 1, 1, 1})
    ->Args({512, 1, 1, 1, 1})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(RecordCacheRW_64MiB, safe, /*is_safe = */ true)
    ->Args({16, 10, 0, 0, 0})
    ->Args({512, 10, 0, 0, 0})
    ->Args({16, 1, 0, 0, 0})
    ->Args({512, 1, 0, 0, 0})
    ->Args({16, 10, 1, 2, 0})
    ->Args({512, 10, 1, 2, 0})
    ->Args({16, 1, 1, 2, 0})
    ->Args({512, 1, 1, 2, 0})
    ->Args({16, 10, 1, 1, 0})
    ->Args({512, 10, 1, 1, 0})
    ->Args({16, 1, 1, 1, 0})
    ->Args({512, 1, 1, 1, 0})
    ->Args({16, 10, 0, 0, 1})
    ->Args({512, 10, 0, 0, 1})
    ->Args({16, 1, 0, 0, 1})
    ->Args({512, 1, 0, 0, 1})
    ->Args({16, 10, 1, 2, 1})
    ->Args({512, 10, 1, 2, 1})
    ->Args({16, 1, 1, 2, 1})
    ->Args({512, 1, 1, 2, 1})
    ->Args({16, 10, 1, 1, 1})
    ->Args({512, 10, 1, 1, 1})
    ->Args({16, 1, 1, 1, 1})
    ->Args({512, 1, 1, 1, 1})
    ->Unit(benchmark::kMillisecond);

}  // namespace
