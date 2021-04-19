#pragma once

#include <filesystem>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <tuple>

#include "buffer_frame.h"
#include "db/page.h"
#include "file_manager.h"
#include "options.h"
#include "page_eviction_strategy.h"
#include "sync_hash_table.h"

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
  // Initializes a BufferManager with the options specified in `options`.
  BufferManager(const BufMgrOptions& options, std::filesystem::path db_path);

  // Writes all dirty pages back and frees resources.
  ~BufferManager();

  // Retrieves the page given by `page_id`, to be held exclusively or not
  // based on the value of `exclusive`. Pages are stored on disk in files with
  // the same name as the page ID (e.g. 1).
  BufferFrame& FixPage(const PhysicalPageId page_id, const bool exclusive);

  // Unfixes a page updating whether it is dirty or not.
  void UnfixPage(BufferFrame& frame, const bool is_dirty);

  // Flushes a page to disk and then unfixes it (the page is not necessarily
  // immediately evicted from the cache).
  void FlushAndUnfixPage(BufferFrame& frame);

  // Writes all dirty pages to disk (without unfixing)
  void FlushDirty();

  // Indicates whether the page given by `page_id` is currently in the buffer
  // manager.
  bool Contains(const PhysicalPageId page_id);

  // Provides access to the underlying FileManager
  FileManager* GetFileManager() const { return file_manager_.get(); }

 private:
  // To support efficient direct I/O, LLSM needs to align its memory buffers to
  // the block size of the underlying file system. On start up, LLSM will
  // attempt to automatically determine the file system's block size. In the
  // unlikely event that LLSM is unable to find the block size, it will fall
  // back to using this default alignment.
  static constexpr size_t kDefaultAlignment = 4096;

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

  // If there are free frames left, returns one of them. Else, returns nullptr.
  BufferFrame* GetFreeFrame();

  // Sets the `data_` field of each frame in frames_.
  void SetFrameDataPointers();

  // The number of pages the buffer manager should keep in memory.
  const size_t buffer_manager_size_;

  // Space in memory to hold the cached pages.
  void* pages_cache_;

  // Space in memory to hold the metadata for each frame.
  std::vector<BufferFrame> frames_;
  std::atomic<uint64_t> free_ptr_;
  std::atomic<bool> no_free_left_;

  // Map from page_id to the buffer frame (if any) that currently holds that
  // page in memory, and a mutex for editing it.
  std::unique_ptr<SyncHashTable<PhysicalPageId, BufferFrame*>> page_to_frame_map_;
  std::mutex map_mutex_;

  // Pointer to a method for determining which (non-fixed) page to evict, and a
  // mutex for editing it.
  std::unique_ptr<PageEvictionStrategy> page_eviction_strategy_;
  std::mutex eviction_mutex_;

  // Pointer to an interface between the BufferManager and pages on disk.
  std::unique_ptr<FileManager> file_manager_;
};

}  // namespace llsm
