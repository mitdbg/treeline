#pragma once

#include "bufmgr/page_memory_allocator.h"
#include "segment_builder.h"

namespace llsm {
namespace pg {

// Used to store data local to a specific worker thread.
class Workspace {
 public:
  PageBuffer& buffer() {
    if (buf_ != nullptr) return buf_;
    buf_ = PageMemoryAllocator::Allocate(
        /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back());
    return buf_;
  }

 private:
  // Lazily allocated. Always large enough to hold the largest segment.
  PageBuffer buf_;
};

}  // namespace pg
}  // namespace llsm
