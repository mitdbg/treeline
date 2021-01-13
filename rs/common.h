#pragma once

#include <cstddef>
#include <cstdint>

namespace rs {

// A CDF coordinate.
template<class KeyType>
struct Coord {
  KeyType x;
  double y;
};

struct SearchBound {
  size_t begin;
  size_t end; // Exclusive.
};

} // namespace rs
