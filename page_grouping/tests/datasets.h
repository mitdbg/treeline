#pragma once

#include <cstdint>
#include <vector>

// Fixture datasets for the tests.
class Datasets {
 public:
  static const std::vector<uint64_t> kSequentialKeys;
  static const std::vector<uint64_t> kUniformKeys;
};
