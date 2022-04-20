#pragma once

#include <pthread.h>

#include <cstdint>
#include <stdexcept>
#include <string>

namespace tl {
namespace affinity {

// Pins the current thread to core `core_id`, returning true if the pin
// succeeded.
static bool PinToCore(uint32_t core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t thread = pthread_self();
  return pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset) == 0;
}

// Unpins the current thread, returning true if the unpin succeeded.
static bool Unpin() {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  pthread_t thread = pthread_self();
  return pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset) == 0;
}

// RAII-helper that pins the current thread to `core_id`.
class PinInScope {
 public:
  PinInScope(uint32_t core_id) {
    if (!PinToCore(core_id)) {
      throw std::runtime_error("Failed to pin thread to core " +
                               std::to_string(core_id));
    }
  }
  ~PinInScope() { Unpin(); }
};

}  // namespace affinity
}  // namespace tl
