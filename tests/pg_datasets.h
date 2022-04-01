#pragma once

#include <cstdint>
#include <random>
#include <vector>

namespace tl {
namespace pg {

// Fixture datasets for the tests.
class Datasets {
 public:
  static std::vector<uint64_t> FloydSample(const size_t num_samples,
                                           uint64_t min_val, uint64_t max_val,
                                           std::mt19937& rng);

  static const std::vector<uint64_t> kSequentialKeys;
  static const std::vector<uint64_t> kUniformKeys;
};

}  // namespace pg
}  // namespace tl
