#include "free_list.h"

#include <cassert>

#include "segment_builder.h"

namespace tl {
namespace pg {

FreeList::FreeList()
    : bytes_allocated_(0),
      list_(TrackingAllocator<SegmentList>(bytes_allocated_)) {
  list_.resize(SegmentBuilder::SegmentPageCounts().size());
}

void FreeList::Add(SegmentId id) {
  std::unique_lock<std::mutex> lock(mutex_);
  AddImpl(id);
}

std::optional<SegmentId> FreeList::Get(const size_t page_count) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto it = SegmentBuilder::PageCountToSegment().find(page_count);
  assert(it != SegmentBuilder::PageCountToSegment().end());
  const size_t file_id = it->second;
  if (list_[file_id].empty()) {
    // No free segments. The caller should allocate a new one.
    return std::optional<SegmentId>();
  }
  const SegmentId free = list_[file_id].front();
  list_[file_id].pop();
  return free;
}

void FreeList::AddBatch(const std::vector<SegmentId>& ids) {
  std::unique_lock<std::mutex> lock(mutex_);
  for (const auto& id : ids) {
    AddImpl(id);
  }
}

void FreeList::AddImpl(SegmentId id) { list_[id.GetFileId()].push(id); }

uint64_t FreeList::GetSizeFootprint() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return bytes_allocated_ + sizeof(*this);
}

uint64_t FreeList::GetNumEntries() const {
  std::unique_lock<std::mutex> lock(mutex_);
  uint64_t total = 0;
  for (const auto& l : list_) {
    total += l.size();
  }
  return total;
}

}  // namespace pg
}  // namespace tl
