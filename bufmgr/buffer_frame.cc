#include "buffer_frame.h"

#include <pthread.h>

namespace tl {

BufferFrame::BufferFrame() {
  data_ = PageMemoryAllocator::Allocate(/* num_pages = */ 1);
  pthread_rwlock_init(&rwlock_, nullptr);
  SetPageId(PhysicalPageId()); // Give it an invalid page id.
  UnsetAllFlags();
  ClearFixCount();
}

// Free all resources.
BufferFrame::~BufferFrame() { pthread_rwlock_destroy(&rwlock_); }

// Initialize a buffer frame based on the page with the specified `page_id`.
void BufferFrame::Initialize(const PhysicalPageId page_id) {
  SetPageId(page_id);
  UnsetAllFlags();
  SetValid();
  ClearFixCount();
}

// Get the page held in the current frame.
Page BufferFrame::GetPage() const { return Page(GetData()); }

// Get a pointer to the data of the page held in the current frame.
void* BufferFrame::GetData() const {
  return reinterpret_cast<void*>(data_.get());
}

// Set/get the page ID of the page held in the current frame.
void BufferFrame::SetPageId(PhysicalPageId page_id) { page_id_ = page_id; }
PhysicalPageId BufferFrame::GetPageId() const { return page_id_; }

// Lock/unlock the current frame, possibly for exclusive access if `exclusive`
// is true.
void BufferFrame::Lock(const bool exclusive) {
  exclusive ? pthread_rwlock_wrlock(&rwlock_) : pthread_rwlock_rdlock(&rwlock_);
}
void BufferFrame::Unlock() { pthread_rwlock_unlock(&rwlock_); }

// Set/Unset/Query the dirty flag of the current frame.
void BufferFrame::SetDirty() { SetFlags(kDirtyFlag); }
void BufferFrame::UnsetDirty() { UnsetFlags(kDirtyFlag); }
bool BufferFrame::IsDirty() const { return flags_ & kDirtyFlag; }

// Set/Unset/Query the eviction flags of the current frame.
void BufferFrame::SetEviction(const uint8_t value) {
  SetFlags(kEvictionFlags & value);
}
void BufferFrame::UnsetEviction() { UnsetFlags(kEvictionFlags); }
uint8_t BufferFrame::GetEviction() const { return flags_ & kEvictionFlags; }
bool BufferFrame::IsNewlyFixed() const { return (GetEviction() == 0); }

// Set/Unset/Query the valid flag of the current frame.
void BufferFrame::SetValid() { SetFlags(kValidFlag); }
void BufferFrame::UnsetValid() { UnsetFlags(kValidFlag); }
bool BufferFrame::IsValid() const { return flags_ & kValidFlag; }

// Unset all flags of the current frame.
void BufferFrame::UnsetAllFlags() { flags_ = 0; }

// Increment/decrement/get/clear the fix count of the current frame.
// IncFixCount/DecFixCount return the new value of the fix count.
size_t BufferFrame::IncFixCount() { return ++fix_count_; }
size_t BufferFrame::DecFixCount() {
  if (fix_count_ == 0) return 0;  // Don't decrement below 0.
  return --fix_count_;
}
size_t BufferFrame::GetFixCount() const { return fix_count_; }
size_t BufferFrame::ClearFixCount() { return fix_count_ = 0; }

}  // namespace tl
