#include <iostream>
#include <stdexcept>

#include "bench/common/data.h"
#include "benchmark/benchmark.h"
#include "treeline/slice.h"
#include "util/packed_map.h"

namespace {

using namespace tl;

void PackedMapInsert(benchmark::State& state, bool shuffle) {
  constexpr size_t kDatasetSizeMiB = 1;
  constexpr size_t kMapSize = (1 << 16) - 8;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  options.shuffle = shuffle;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  size_t inserted = 0;
  bool success = false;

  for (auto _ : state) {
    PackedMap<kMapSize> map;
    for (const auto& record : dataset) {
      inserted += (success = map.Insert(
                       reinterpret_cast<const uint8_t*>(record.key().data()),
                       record.key().size(),
                       reinterpret_cast<const uint8_t*>(record.value().data()),
                       record.value().size()));

      if (!success) break;
    }
  }
  state.SetBytesProcessed(inserted * options.record_size);
}

void PackedMapAppend(benchmark::State& state, bool shuffle,
                     bool perform_checks) {
  constexpr size_t kDatasetSizeMiB = 1;
  constexpr size_t kMapSize = (1 << 16) - 8;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  options.shuffle = shuffle;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  size_t inserted = 0;
  bool success = false;

  for (auto _ : state) {
    PackedMap<kMapSize> map;
    for (const auto& record : dataset) {
      inserted += (success = map.Append(
                       reinterpret_cast<const uint8_t*>(record.key().data()),
                       record.key().size(),
                       reinterpret_cast<const uint8_t*>(record.value().data()),
                       record.value().size(), perform_checks));

      if (!success) break;
    }
  }
  state.SetBytesProcessed(inserted * options.record_size);
}

BENCHMARK_CAPTURE(PackedMapInsert, in_order, /*shuffle=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512);

BENCHMARK_CAPTURE(PackedMapInsert, shuffled, /*shuffle=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512);

BENCHMARK_CAPTURE(PackedMapAppend, in_order_check, /*shuffle=*/false,
                  /*perform_checks=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512);

BENCHMARK_CAPTURE(PackedMapAppend, in_order_no_check, /*shuffle=*/false,
                  /*perform_checks=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512);

BENCHMARK_CAPTURE(PackedMapAppend, shuffled, /*shuffle=*/true,
                  /*perform_checks=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512);

}  // namespace
