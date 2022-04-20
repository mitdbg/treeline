#pragma once

#include "buffer_frame.h"
namespace tl {

// A strategy for evicting in-memory pages back to disk.
class PageEvictionStrategy {
 public:
  // Frees all resources.
  virtual ~PageEvictionStrategy() { return; }

  // Makes the page held by `frame` a candidate for eviction.
  virtual void Insert(BufferFrame* frame) = 0;

  // Ensures the page held by `frame` is no longer a candidate for eviction.
  virtual void Delete(BufferFrame* frame) = 0;

  // Evicts a page previously marked as a candidate for eviction (if any).
  virtual BufferFrame* Evict() = 0;

  // Prints the state of the eviction strategy
  virtual void PrintState() = 0;
};

}  // namespace tl
