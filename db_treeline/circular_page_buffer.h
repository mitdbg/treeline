#pragma once

#include "../bufmgr/page_memory_allocator.h"
#include "persist/page.h"

namespace tl {
namespace pg {

class CircularPageBuffer {
 public:
  CircularPageBuffer(size_t num_pages)
      : buf_(PageMemoryAllocator::Allocate(num_pages)),
        base_(buf_.get()),
        num_pages_(num_pages),
        head_idx_(0),
        tail_idx_(0),
        free_pages_(num_pages) {}

  size_t NumFreePages() const { return free_pages_; }
  size_t NumAllocatedPages() const { return num_pages_ - free_pages_; }

  void* Allocate() {
    assert(free_pages_ > 0);
    void* to_return = base_ + (head_idx_ * pg::Page::kSize);
    ++head_idx_;
    --free_pages_;
    if (head_idx_ >= num_pages_) {
      head_idx_ = 0;
    }
    return to_return;
  }

  void Free() {
    assert(NumAllocatedPages() > 0);
    assert(tail_idx_ != head_idx_);
    ++tail_idx_;
    ++free_pages_;
    if (tail_idx_ >= num_pages_) {
      tail_idx_ = 0;
    }
  }

 private:
  PageBuffer buf_;
  char* base_;
  size_t num_pages_;
  // Pages are allocated at `head_idx_` and are freed at `tail_idx_`.
  // Having `head_idx_ == tail_idx_` means that no pages are allocated.
  size_t head_idx_, tail_idx_;
  size_t free_pages_;
};

}  // namespace pg
}  // namespace tl
