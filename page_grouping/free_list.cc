#include "free_list.h"

#include <cassert>

#include "segment_builder.h"

namespace llsm {
namespace pg {

FreeList::FreeList() {
  list_.resize(SegmentBuilder::kSegmentPageCounts.size());
}

void FreeList::Add(SegmentId id) {
  list_[id.GetFileId()].push(id);
}

std::optional<SegmentId> FreeList::Get(const size_t page_count) {
  const auto it = SegmentBuilder::kPageCountToSegment.find(page_count);
  assert(it != SegmentBuilder::kPageCountToSegment.end());
  const size_t file_id = it->second;
  if (list_[file_id].empty()) {
    // No free segments. The caller should allocate a new one.
    return std::optional<SegmentId>();
  }
  const SegmentId free = list_[file_id].front();
  list_[file_id].pop();
  return free;
}

}  // namespace pg
}  // namespace llsm
