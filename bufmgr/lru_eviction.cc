#include "lru_eviction.h"

#include <inttypes.h>

#include <iostream>

namespace tl {

// Create a new eviction strategy.
LRUEviction::LRUEviction(size_t num_elements) {
  lru_ = std::make_unique<HashQueue<BufferFrame*>>(num_elements);
}

// Free all resources.
LRUEviction::~LRUEviction() {}

// Makes the page held by `frame` a candidate for eviction.
void LRUEviction::Insert(BufferFrame* frame) {
  lru_->Enqueue(frame);
  frame->SetEviction(kHasBeenInLru);
}

// Ensures the page held by `frame` is no longer a candidate for eviction.
void LRUEviction::Delete(BufferFrame* frame) {
  uint8_t eviction_flags = frame->GetEviction();

  if (HasBeenInLru(eviction_flags)) {
    lru_->Delete(frame);
  }
}

// Evicts a page previously marked as a candidate for eviction (if any),
// following the LRU eviction strategy.
BufferFrame* LRUEviction::Evict() {
  BufferFrame* evicted = nullptr;

  if (!lru_->IsEmpty()) {
    evicted = lru_->Dequeue();
    evicted->UnsetEviction();
  }

  return evicted;
}

// Prints the state of the eviction strategy, namely the contents of the LRU
// queue.
void LRUEviction::PrintState() {
  std::cout << "----------------------------" << std::endl
            << "Printing the state of LRU..." << std::endl;

  for (auto it = lru_->begin(); it != lru_->end(); ++it) {
    std::cout << (*it)->GetPageId() << std::endl;
  }

  std::cout << "----------------------------" << std::endl;
}

}  // namespace tl
