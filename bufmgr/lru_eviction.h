#pragma once

#include <stdint.h>

#include <list>
#include <set>

#include "buffer_frame.h"
#include "../util/hash_queue.h"
#include "page_eviction_strategy.h"

namespace tl {

// The LRU strategy for evicting in-memory pages back to disk.
//
// Upon inserting a page, it is entered into an LRU queue. Upon deleting
// a page, it is removed from the queue, if it was in it. Upon needing to evict
// a page, we evict the head of the LRU queue, if it is non-empty; else, we
// return nullptr.
class LRUEviction : public PageEvictionStrategy {
 public:
  // Create a new eviction strategy.
  LRUEviction(size_t num_elements);

  // Free all resources.
  ~LRUEviction();

  // Makes the page held by `frame` a candidate for eviction.
  void Insert(BufferFrame* frame);

  // Ensures the page held by `frame` is no longer a candidate for eviction.
  void Delete(BufferFrame* frame);

  // Evicts a page previously marked as a candidate for eviction (if any),
  // following the LRU eviction strategy.
  BufferFrame* Evict();

  // Prints the state of the eviction strategy, namely the contents of the LRU
  // queue.
  void PrintState();

 private:
  // Flag for whether the page in a frame has previously been in the LRU
  // eviction queue.
  static const uint8_t kHasBeenInLru = 2;  // 0000 0010

  // Queries whether the page in a frame has previously been in the LRU
  // eviction queue.
  bool HasBeenInLru(uint8_t eviction_flags) const {
    return eviction_flags & kHasBeenInLru;
  }

  // The LRU eviction queue.
  std::unique_ptr<HashQueue<BufferFrame*>> lru_;
};

}  // namespace tl
