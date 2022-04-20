#include "twoqueue_eviction.h"

#include <inttypes.h>

#include <iostream>

namespace tl {

// Create a new eviction strategy.
TwoQueueEviction::TwoQueueEviction(size_t num_elements) {
  fifo_ = new HashQueue<BufferFrame*>(num_elements);
  lru_ = new HashQueue<BufferFrame*>(num_elements);
}

// Free all resources.
TwoQueueEviction::~TwoQueueEviction() {
  delete fifo_;
  delete lru_;
}

// Makes the page held by `frame` a candidate for eviction.
void TwoQueueEviction::Insert(BufferFrame* frame) {
  uint8_t eviction_flags = frame->GetEviction();

  if (!HasBeenInFifo(eviction_flags)) {
    // If it has not been in the FIFO, put into FIFO
    fifo_->Enqueue(frame);
    frame->SetEviction(kHasBeenInFifo);
  } else {
    // If it has been in the FIFO, put into LRU
    lru_->Enqueue(frame);
    frame->SetEviction(kHasBeenInLru);
  }
}

// Ensures the page held by `frame` is no longer a candidate for eviction.
void TwoQueueEviction::Delete(BufferFrame* frame) {
  uint8_t eviction_flags = frame->GetEviction();

  if (HasBeenInLru(eviction_flags)) {
    lru_->Delete(frame);
  } else if (HasBeenInFifo(eviction_flags)) {
    fifo_->Delete(frame);
  }
}

// Evicts a page previously marked as a candidate for eviction (if any),
// following the 2Q eviction strategy.
BufferFrame* TwoQueueEviction::Evict() {
  BufferFrame* evicted = nullptr;

  // Evict first from the FIFO if possible
  if (!fifo_->IsEmpty()) {
    evicted = fifo_->Dequeue();
  } else if (!lru_->IsEmpty()) {
    evicted = lru_->Dequeue();
  }

  if (evicted != nullptr) evicted->UnsetEviction();

  return evicted;
}

// Prints the state of the eviction strategy, namely the contents of the two
// queues.
void TwoQueueEviction::PrintState() {
  std::cout << "---------------------------" << std::endl
            << "Printing the state of 2Q..." << std::endl;

  std::cout << "Printing FIFO contents" << std::endl;
  for (auto it = fifo_->begin(); it != fifo_->end(); ++it) {
    std::cout << (*it)->GetPageId() << std::endl;
  }

  std::cout << "Printing LRU contents" << std::endl;
  for (auto it = lru_->begin(); it != lru_->end(); ++it) {
    std::cout << (*it)->GetPageId() << std::endl;
  }

  std::cout << "---------------------------" << std::endl;
}

}  // namespace tl
