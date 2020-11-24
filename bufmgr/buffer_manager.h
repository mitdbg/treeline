
#pragma once

#include <iostream>
#include <list>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "buffer_frame.h"
#include "file_manager.h"
#include "page_eviction_strategy.h"

namespace llsm {
// Manages an in-memory buffer of pages.
class BufferManager {
  // The page size in bytes.
  static const size_t kPageSizeBytes = 4096;

  // Default number of pages in buffer manager.
  static const size_t kDefaultBufMgrSize = 16384;

 public:
  // Initialize a BufferManager to keep up to `buffer_manager_size` frames in
  // main memory. Bypasses file system cache if `use_direct_io` is true.
  BufferManager();
  BufferManager(const size_t buffer_manager_size);
  BufferManager(const bool use_direct_io);
  BufferManager(const size_t buffer_manager_size, const bool use_direct_io);

  // Writes all dirty pages back and frees resources.
  ~BufferManager();

  // Retrieve the page given by `page_id`, to be held exclusively or not
  // based on the value of `exclusive`. Pages are stored on disk in files with
  // the same name as the page ID (e.g. 1).
  BufferFrame& FixPage(const uint64_t page_id, const bool exclusive);

  // Unfix a page updating whether it is dirty or not.
  void UnfixPage(BufferFrame& frame, const bool is_dirty);

 private:
  // Writes the page held by `frame` to disk.
  void WritePageOut(BufferFrame* frame) const;

  // Reads a page from disk into `frame`.
  void ReadPageIn(BufferFrame* frame);

  // Locks/unlocks the mutex for editing page_to_frame_map_.
  void LockMapMutex() { map_mutex_.lock(); }
  void UnlockMapMutex() { map_mutex_.unlock(); }

  // Locks/unlocks the muutex for editing eviction_strategy_.
  void LockEvictionMutex() { eviction_mutex_.lock(); }
  void UnlockEvictionMutex() { eviction_mutex_.unlock(); }

  // Creates a new frame and specifies that it will hold the page with
  // `page_id`.
  BufferFrame* CreateFrame(const uint64_t page_id);

  // Resets an exisiting frame to hold the page with `new_page_id`.
  void ResetFrame(BufferFrame* frame, const uint64_t new_page_id);

  // The number of pages the buffer manager should keep in memory.
  size_t buffer_manager_size_;

  // Space in memory to hold the cached pages.
  void* pages_cache_;

  // Pointers to available page-sized chunks in memory, for fixing pages from
  // disk.
  std::list<void*> free_pages_;

  // Map from page_id to the buffer frame (if any) that currently holds that
  // page in memory, and a mutex for editing it.
  std::unordered_map<uint64_t, BufferFrame*> page_to_frame_map_;
  std::mutex map_mutex_;

  // Pointer to a method for determining which (non-fixed) page to evict, and a
  // mutex for editing it.
  PageEvictionStrategy* page_eviction_strategy_;
  std::mutex eviction_mutex_;

  // Pointer to an interface between the BufferManager and pages on disk.
  FileManager* file_manager_;
};

}  // namespace llsm
