#include "treeline/pg_stats.h"

namespace tl {
namespace pg {

std::mutex PageGroupedDBStats::class_mutex_;
PageGroupedDBStats PageGroupedDBStats::global_;

PageGroupedDBStats::PageGroupedDBStats() { Reset(); }

void PageGroupedDBStats::PostToGlobal() const {
  std::unique_lock<std::mutex> lock(class_mutex_);
  global_.cache_hits_ += cache_hits_;
  global_.cache_misses_ += cache_misses_;
  global_.cache_clean_evictions_ += cache_clean_evictions_;
  global_.cache_dirty_evictions_ += cache_dirty_evictions_;

  global_.overflows_created_ += overflows_created_;
  global_.rewrites_ += rewrites_;
  global_.rewrite_input_pages_ += rewrite_input_pages_;
  global_.rewrite_output_pages_ += rewrite_output_pages_;

  global_.segments_ = segments_;
  global_.free_list_entries_ += free_list_entries_;
  global_.free_list_bytes_ += free_list_bytes_;
  global_.segment_index_bytes_ += segment_index_bytes_;
  global_.lock_manager_bytes_ += lock_manager_bytes_; 
  global_.cache_bytes_ += cache_bytes_;

  global_.overfetched_pages_ += overfetched_pages_;
}

void PageGroupedDBStats::Reset() {
  cache_hits_ = 0;
  cache_misses_ = 0;
  cache_clean_evictions_ = 0;
  cache_dirty_evictions_ = 0;

  overflows_created_ = 0;
  rewrites_ = 0;
  rewrite_input_pages_ = 0;
  rewrite_output_pages_ = 0;

  segments_ = 0;
  free_list_entries_ = 0;
  free_list_bytes_ = 0;
  segment_index_bytes_ = 0;
  lock_manager_bytes_ = 0;
  cache_bytes_ = 0;

  overfetched_pages_ = 0;
}

}  // namespace pg
}  // namespace tl
