#pragma once

#include <mutex>
#include <optional>
#include <queue>
#include <vector>

#include "persist/segment_id.h"

namespace llsm {
namespace pg {

// Keeps track of available locations on disk for segments of different sizes.
// This class' methods are thread-safe.
class FreeList {
 public:
  FreeList();
  void Add(SegmentId id);
  void AddBatch(const std::vector<SegmentId>& ids);
  std::optional<SegmentId> Get(size_t page_count);

 private:
  void AddImpl(SegmentId id);

  std::mutex mutex_;
  std::vector<std::queue<SegmentId>> list_;
};

}  // namespace pg
}  // namespace llsm
