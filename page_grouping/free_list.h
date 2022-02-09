#pragma once

#include <optional>
#include <queue>
#include <vector>

#include "persist/segment_id.h"

namespace llsm {
namespace pg {

class FreeList {
 public:
  FreeList();
  void Add(SegmentId id);
  std::optional<SegmentId> Get(size_t page_count);

 private:
  std::vector<std::queue<SegmentId>> list_;
};

}  // namespace pg
}  // namespace llsm
