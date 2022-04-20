#include "pg_datasets.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <vector>

namespace tl {
namespace pg {

// Uniformly selects `num_samples` samples from the range [min_val, max_val]
// without replacement.
std::vector<uint64_t> Datasets::FloydSample(const size_t num_samples,
                                            uint64_t min_val, uint64_t max_val,
                                            std::mt19937& rng) {
  std::unordered_set<uint64_t> samples;
  samples.reserve(num_samples);
  for (uint64_t curr = max_val - num_samples + 1; curr <= max_val; ++curr) {
    std::uniform_int_distribution<uint64_t> dist(min_val, curr);
    const uint64_t next = dist(rng);
    auto res = samples.insert(next);
    if (!res.second) {
      samples.insert(curr);
    }
  }
  assert(samples.size() == num_samples);
  return std::vector<uint64_t>(samples.begin(), samples.end());
}

const std::vector<uint64_t> Datasets::kSequentialKeys =
    ([](const size_t range) {
      std::vector<uint64_t> results;
      results.resize(range);
      std::iota(results.begin(), results.end(), 1ULL);
      return results;
    })(1000);

const std::vector<uint64_t> Datasets::kUniformKeys =
    ([](const size_t num_samples, uint64_t min_val, uint64_t max_val) {
      std::mt19937 prng(42);
      auto res = Datasets::FloydSample(num_samples, min_val, max_val, prng);
      std::sort(res.begin(), res.end());
      return res;
    })(1000, 1, 1000000);

}  // namespace pg
}  // namespace tl
