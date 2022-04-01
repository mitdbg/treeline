#pragma once

#include <immintrin.h>

#include <random>

namespace tl {
namespace pg {

// Used for implementing randomized exponential backoff as a spin-wait.
class RandExpBackoff {
 public:
  explicit RandExpBackoff(uint32_t saturate_at)
      : attempts_(0), saturate_at_(saturate_at) {}

  // Call this method to spin wait. The maximum number of spin waits will
  // increase exponentially as a function of how many times this method is
  // called, up to `saturate_at_` times.
  void Wait() {
    if (attempts_ < saturate_at_) {
      ++attempts_;
    }

    const uint32_t max_spin_cycles = 10 * (1UL << attempts_);
    std::uniform_int_distribution<uint32_t> dist(1, max_spin_cycles);

    uint32_t spin_for = dist(prng_);
    while (spin_for > 0) {
      _mm_pause();
      --spin_for;
    }
  }

  void Reset() { attempts_ = 0; }

 private:
  static thread_local std::mt19937 prng_;
  uint32_t attempts_;
  uint32_t saturate_at_;
};

}  // namespace pg
}  // namespace tl
