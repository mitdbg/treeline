#pragma once

#include <cstdint>
#include <mutex>

namespace llsm {
namespace pg {

// This class stores counters used by the page-grouped LLSM database.
//
// This class only supports counters that can be aggregated across threads. The
// counters are meant to be manipulated using thread local instances and then
// aggregated when needed (e.g., when an experiment has completed).
class PageGroupedDBStats {
 public:
  static PageGroupedDBStats& Local() {
    static thread_local PageGroupedDBStats local;
    return local;
  }

  template <typename Callable>
  static void RunOnGlobal(const Callable& c) {
    std::unique_lock<std::mutex> lock(class_mutex_);
    c(global_);
  }

  PageGroupedDBStats(const PageGroupedDBStats&) = delete;
  PageGroupedDBStats& operator=(const PageGroupedDBStats&) = delete;

  uint64_t GetCacheHits() const { return cache_hits_; }
  uint64_t GetCacheMisses() const { return cache_misses_; }
  uint64_t GetCacheCleanEvictions() const { return cache_clean_evictions_; }
  uint64_t GetCacheDirtyEvictions() const { return cache_dirty_evictions_; }

  void BumpCacheHits() { ++cache_hits_; }
  void BumpCacheMisses() { ++cache_misses_; }
  void BumpCacheCleanEvictions() { ++cache_clean_evictions_; }
  void BumpCacheDirtyEvictions() { ++cache_dirty_evictions_; }

  // Threads must call this method to post their counter values to the global
  // `PageGroupedDBStats` instance.
  void PostToGlobal() const;
  void Reset();

 private:
  PageGroupedDBStats();

  static std::mutex class_mutex_;
  static PageGroupedDBStats global_;

  // Record-cache related counters.
  uint64_t cache_hits_;
  uint64_t cache_misses_;
  uint64_t cache_clean_evictions_;
  uint64_t cache_dirty_evictions_;
};

}  // namespace pg
}  // namespace llsm
