#pragma once

#include <chrono>

namespace tl {
namespace bench {

template<typename Callable>
static std::chrono::nanoseconds MeasureRunTime(Callable callable) {
  auto start = std::chrono::steady_clock::now();
  callable();
  auto end = std::chrono::steady_clock::now();
  return end - start;
}

}  // namespace bench
}  // namespace tl
