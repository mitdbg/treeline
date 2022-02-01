#pragma once

#include <vector>

#include "bufmgr/page_memory_allocator.h"
#include "segment_builder.h"

namespace llsm {
namespace pg {

// Used to store data local to a specific worker thread.
class Workspace {
 public:
  Workspace() {
    read_counts_.resize(SegmentBuilder::kSegmentPageCounts.back(), 0);
  }

  PageBuffer& buffer() {
    if (buf_ != nullptr) return buf_;
    // Add one for the overflow page.
    buf_ = PageMemoryAllocator::Allocate(
        /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back() + 1);
    return buf_;
  }

  const std::vector<size_t>& read_counts() const { return read_counts_; }

  void BumpReadCount(const size_t num_contiguous_pages_read) {
    ++read_counts_[num_contiguous_pages_read - 1];
  }

 private:
  // Lazily allocated. Always large enough to hold the largest segment.
  PageBuffer buf_;

  // Tracks the number of page reads of different sizes. The index (plus one)
  // represents the number of pages read (e.g., index 0 means 1 page, index 1
  // means 2 pages, etc.).
  std::vector<size_t> read_counts_;
};

}  // namespace pg
}  // namespace llsm
