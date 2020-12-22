
#pragma once

#include <iostream>
#include <list>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "buffer_frame.h"
#include "file_manager.h"
#include "page_eviction_strategy.h"
#include "llsm/options.h"

namespace llsm {

// A manager for an in-memory cache of pages.
//
// The client can "fix" and "unfix" pages as they are needed, and the
// `BufferManager` will attempt to minimize disk IO following these requests.
//
// A page is only evicted back to disk if it is unfixed and there is no space
// in the in-memory buffer to serve an incoming fix request for some page not
// currently buffered.
//
// To manage page eviction, this class uses the "two-queue" strategy. Upon
// unfixing a page, it is either entered into a FIFO queue (if this is the first
// time it was unfixed since first entering the buffer), or into an LRU queue.
// Upon fixing a page, it is removed from these queues, if it were in any. Upon
// needing to evict a page, we evict the head of the FIFO queue; if the FIFO
// queue is empty, we evict the head of the LRU queue; else, we wait.
//
// This class is thread-safe; mutexes are used to serialize accesses to critical
// data structures.
class BufferManager {
 
 public:
  // Initialize a BufferManager with the options specified in `options`.
  BufferManager(const BufMgrOptions options, std::string db_path);
  
  // Writes all dirty pages back and frees resources.
  ~BufferManager();

  // Retrieve the page given by `page_id`, to be held exclusively or not
  // based on the value of `exclusive`. Pages are stored on disk in files with
  // the same name as the page ID (e.g. 1).
  BufferFrame& FixPage(const uint64_t page_id, const bool exclusive);

  // Unfix a page updating whether it is dirty or not.
  void UnfixPage(BufferFrame& frame, const bool is_dirty);

  // Write all dirty pages to disk (without unfixing)
  void FlushDirty();

 private:
  // Writes the page held by `frame` to disk.
  void WritePageOut(BufferFrame* frame) const;

  // Reads a page from disk into `frame`.
  void ReadPageIn(BufferFrame* frame);

  // Locks/unlocks the mutex for editing free_pages_.
  void LockFreePagesMutex() { free_pages_mutex_.lock(); }
  void UnlockFreePagesMutex() { free_pages_mutex_.unlock(); }

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

  // Options provided upon creation
  const BufMgrOptions options_;

  // The number of pages the buffer manager should keep in memory.
  const size_t buffer_manager_size_;

  // The size of each page
  const size_t page_size_;

  // Space in memory to hold the cached pages.
  void* pages_cache_;

  // Pointers to available page-sized chunks in memory, for fixing pages from
  // disk.
  std::list<void*> free_pages_;
  std::mutex free_pages_mutex_;

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
