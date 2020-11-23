// Draft version based on
// https://github.com/mschrimpf/dbimpl/blob/master/src/database/buffer/BufferFrame.hpp

#pragma once

#include <pthread.h>
#include <stdint.h>

#include <atomic>

namespace llsm {

// Wraps an in-memory page with necessary metadata for use with BufferManager.
class BufferFrame {
  // Constants for flags. 1 bit for page dirtiness, 2 bits to be used by the
  // page eviction strategy.
  static const uint8_t kDirtyFlag = 1;      // 0000 0001
  static const uint8_t kEvictionFlags = 6;  // 0000 0110
  static const uint8_t kAllFlags = 255;     // 1111 1111

 public:
  // Initialize a buffer frame based on the page with the specified `page_id`,
  // which is pointed to by `data`.
  BufferFrame(const uint64_t page_id, void* data);

  // Free all resources.
  ~BufferFrame();

  // Return the data contained in the page held in the current frame.
  void* GetData() const;

  // Set/get the page ID of the page held in the current frame.
  void SetPageId(const uint64_t page_id);
  uint64_t GetPageId() const;

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

  // Unset all flags of the current frame.
  void UnsetAllFlags();

  // Increment/decrement/get the fix count of the current frame.
  // IncFixCount/DecFixCount return the new value of the fix count.
  size_t IncFixCount();
  size_t DecFixCount();
  size_t GetFixCount() const;

 private:
  // Set/Unset the specified flags of the current frame.
  void SetFlags(const uint8_t flags) { flags_ |= flags; }
  void UnsetFlags(const uint8_t flags) { flags_ &= ~flags; }

  // The data of the page held by the frame.
  void* data_;

  // The id of the page held by the frame.
  uint64_t page_id_;

  // A read/write lock to be acquired by a thread accessing the page in the
  // frame.
  pthread_rwlock_t rwlock_;

  // Flags for dirtiness and eviction management.
  std::atomic<uint8_t> flags_;

  // A count of how many threads have the current frame as fixed.
  std::atomic<size_t> fix_count_;
};

}  // namespace llsm
