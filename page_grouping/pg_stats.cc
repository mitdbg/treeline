#include "llsm/pg_stats.h"

namespace llsm {
namespace pg {

std::mutex PageGroupedDBStats::class_mutex_;
PageGroupedDBStats PageGroupedDBStats::global_;

PageGroupedDBStats::PageGroupedDBStats()
    : cache_hits_(0),
      cache_misses_(0),
      cache_clean_evictions_(0),
      cache_dirty_evictions_(0) {}

void PageGroupedDBStats::PostToGlobal() const {
  std::unique_lock<std::mutex> lock(class_mutex_);
  global_.cache_hits_ += cache_hits_;
  global_.cache_misses_ += cache_misses_;
  global_.cache_clean_evictions_ += cache_clean_evictions_;
  global_.cache_dirty_evictions_ += cache_dirty_evictions_;
}

void PageGroupedDBStats::Reset() {
  cache_hits_ = 0;
  cache_misses_ = 0;
  cache_clean_evictions_ = 0;
  cache_dirty_evictions_ = 0;
}

}  // namespace pg
}  // namespace llsm
