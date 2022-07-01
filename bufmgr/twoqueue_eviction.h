#pragma once

#include <stdint.h>

#include <list>
#include <set>

#include "buffer_frame.h"
#include "../util/hash_queue.h"
#include "page_eviction_strategy.h"
namespace tl {

// The 2Q strategy for evicting in-memory pages back to disk.
//
// Upon inserting a page, it is either entered into a FIFO queue (if this is the
// first time it was inserted into the strategy), or into an LRU
// queue. Upon deleting a page, it is removed from these queues, if it were in
// any. Upon needing to evict a page, we evict the head of the FIFO queue; if
// the FIFO queue is empty, we evict the head of the LRU queue; else, we return
// nullptr.
class TwoQueueEviction : public PageEvictionStrategy {
 public:
  // Create a new eviction strategy.
  TwoQueueEviction(size_t num_elements);

  // Free all resources.
  ~TwoQueueEviction();

  // Makes the page held by `frame` a candidate for eviction.
  void Insert(BufferFrame* frame);

  // Ensures the page held by `frame` is no longer a candidate for eviction.
  void Delete(BufferFrame* frame);

  // Evicts a page previously marked as a candidate for eviction (if any),
  // following the 2Q eviction strategy.
  BufferFrame* Evict();

  // Prints the state of the eviction strategy, namely the contents of the two
  // queues.
  void PrintState();

 private:
  // Flag for whether the page in a frame has previously been in the FIFO
  // eviction queue.
  static const uint8_t kHasBeenInFifo = 4;  // 0000 0100

  // Flag for whether the page in a frame has previously been in the LRU
  // eviction queue.
  static const uint8_t kHasBeenInLru = 2;  // 0000 0010
  
  // Queries whether the page in a frame has previously been in the FIFO
  // eviction queue.
  bool HasBeenInFifo(uint8_t eviction_flags) const {
    return eviction_flags & kHasBeenInFifo;
  }

  // Queries whether the page in a frame has previously been in the LRU
  // eviction queue.
  bool HasBeenInLru(uint8_t eviction_flags) const {
    return eviction_flags & kHasBeenInLru;
  }

  // Two lists to be used as the FIFO and LRU eviction queues of the 2Q eviction
  // strategy.
  HashQueue<BufferFrame*>* fifo_;
  HashQueue<BufferFrame*>* lru_;
};

}  // namespace tl
