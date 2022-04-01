#pragma once

#include <pthread.h>
#include <stdint.h>

#include <atomic>

#include "db/page.h"
#include "page_memory_allocator.h"

namespace tl {

// A wrapper for memory pages, containing metadata used by the buffer manager.
class BufferFrame {
  // Constants for flags. 1 bit for page dirtiness, 2 bits to be used by the
  // page eviction strategy.
  static const uint8_t kDirtyFlag = 1;      // 0000 0001
  static const uint8_t kEvictionFlags = 6;  // 0000 0110
  static const uint8_t kValidFlag = 8;      // 0000 1000
  static const uint8_t kAllFlags = 255;     // 1111 1111

 public:
  // Create an invalid buffer frame.
  BufferFrame();

  // Free all resources.
  ~BufferFrame();

  // Initialize a buffer frame based on the page with the specified `page_id`.
  void Initialize(const PhysicalPageId page_id);

  // Return the page held in the current frame.
  Page GetPage() const;

  // Get a pointer to the data of the page held in the current frame.
  void* GetData() const;

  // Set/get the page ID of the page held in the current frame.
  void SetPageId(const PhysicalPageId page_id);
  PhysicalPageId GetPageId() const;

  // Lock/unlock the current frame, possibly for exclusive access if `exclusive`
  // is true.
  void Lock(const bool exclusive);
  void Unlock();

  // Set/Unset/Query the dirty flag of the current frame.
  void SetDirty();
  void UnsetDirty();
  bool IsDirty() const;

  // Set/Unset/Query the eviction flags of the current frame.
  void SetEviction(const uint8_t value);
  void UnsetEviction();
  uint8_t GetEviction() const;
  bool IsNewlyFixed() const;

  // Set/Unset/Query the valid flag of the current frame.
  void SetValid();
  void UnsetValid();
  bool IsValid() const;

  // Unset all flags of the current frame.
  void UnsetAllFlags();

  // Increment/decrement/get/clear the fix count of the current frame.
  // IncFixCount/DecFixCount return the new value of the fix count.
  size_t IncFixCount();
  size_t DecFixCount();
  size_t GetFixCount() const;
  size_t ClearFixCount();

 private:
  // Set/Unset the specified flags of the current frame.
  void SetFlags(const uint8_t flags) { flags_ |= flags; }
  void UnsetFlags(const uint8_t flags) { flags_ &= ~flags; }

  // The data of the page held by the frame.
  PageBuffer data_;

  // The id of the page held by the frame.
  PhysicalPageId page_id_;

  // A read/write lock to be acquired by a thread accessing the page in the
  // frame.
  pthread_rwlock_t rwlock_;

  // Flags for dirtiness and eviction management.
  std::atomic<uint8_t> flags_;

  // A count of how many threads have the current frame as fixed.
  std::atomic<size_t> fix_count_;
};

}  // namespace tl
