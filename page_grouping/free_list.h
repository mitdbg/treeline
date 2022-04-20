#pragma once

#include <deque>
#include <mutex>
#include <optional>
#include <queue>
#include <scoped_allocator>
#include <vector>

#include "../util/tracking_allocator.h"
#include "persist/segment_id.h"

namespace tl {
namespace pg {

// Keeps track of available locations on disk for segments of different sizes.
// This class' methods are thread-safe.
class FreeList {
 public:
  FreeList();
  void Add(SegmentId id);
  void AddBatch(const std::vector<SegmentId>& ids);
  std::optional<SegmentId> Get(size_t page_count);

  uint64_t GetSizeFootprint() const;
  uint64_t GetNumEntries() const;

 private:
  void AddImpl(SegmentId id);

  mutable std::mutex mutex_;
  using SegmentList =
      std::queue<SegmentId,
                 std::deque<SegmentId, TrackingAllocator<SegmentId>>>;
  uint64_t bytes_allocated_;
  std::vector<SegmentList,
              std::scoped_allocator_adaptor<TrackingAllocator<SegmentList>>>
      list_;
};

}  // namespace pg
}  // namespace tl
