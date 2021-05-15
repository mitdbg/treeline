#include "buffer_manager.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <mutex>

#include "file_manager.h"
#include "twoqueue_eviction.h"

namespace llsm {

// Initializes a BufferManager to keep up to `options.buffer_pool_size /
// Page::kSize` frames in main memory. Bypasses file system cache if
// `options.use_direct_io` is true.
BufferManager::BufferManager(const BufMgrOptions& options,
                             std::filesystem::path db_path)
    : buffer_manager_size_(options.buffer_pool_size / Page::kSize),
      num_misses_(0),
      cumulative_misses_time_ns_(0) {
  page_to_frame_map_ =
      std::make_unique<SyncHashTable<PhysicalPageId, BufferFrame*>>(
          buffer_manager_size_, /*num_partitions = */ 1);

  page_eviction_strategy_ =
      std::make_unique<TwoQueueEviction>(buffer_manager_size_);

  // Create the frames and insert them into the page eviction strategy, from
  // where they will be retrieved to be fixed.
  for (size_t i = 0; i < buffer_manager_size_; ++i) {
    page_eviction_strategy_->Insert(new BufferFrame());
  }

  if (!options.simulation_mode) {
    file_manager_ = std::make_unique<FileManager>(options, std::move(db_path));
  } else {
    file_manager_ = nullptr;
  }
}

// Writes all dirty pages back and frees resources.
BufferManager::~BufferManager() {
  // Clear eviction strategy and delete any invalid frames, since they are not
  // in the page_to_frame_map_.
  BufferFrame* frame;
  while ((frame = page_eviction_strategy_->Evict()) != nullptr) {
    if (!frame->IsValid()) delete frame;
  }

  FlushDirty(/*also_delete = */ true);
}

// Retrieves the page given by `page_id`, to be held exclusively or not
// based on the value of `exclusive`.
BufferFrame& BufferManager::FixPage(const PhysicalPageId page_id,
                                    const bool exclusive,
                                    const bool is_newly_allocated) {
  BufferFrame* frame = nullptr;
  bool success;

  // Check if page is already loaded in some frame.
  LockMapMutex();
  LockEvictionMutex();
  auto frame_lookup = page_to_frame_map_->UnsafeLookup(page_id, &frame);

  // If yes, load the corresponding frame
  if (frame_lookup) {
    if (frame->IncFixCount() == 1) page_eviction_strategy_->Delete(frame);
    UnlockEvictionMutex();
    UnlockMapMutex();

  } else {  // If not, we have to bring it in from disk.
    UnlockEvictionMutex();
    UnlockMapMutex();

    auto start = std::chrono::steady_clock::now();

    // Block here until you can evict something
    while (frame == nullptr) {
      LockMapMutex();
      LockEvictionMutex();
      frame = page_eviction_strategy_->Evict();
      if (frame != nullptr) {
        page_to_frame_map_->UnsafeErase(frame->GetPageId());
      }
      UnlockEvictionMutex();
      UnlockMapMutex();
    }

    // Write out evicted page if necessary
    if (frame->IsDirty()) {
      WritePageOut(frame);
      frame->UnsetDirty();
    }

    // Initialize and populate the frame
    frame->Initialize(page_id);
    frame->IncFixCount();
    if (!is_newly_allocated) ReadPageIn(frame);

    // Insert the frame into the map.
    LockMapMutex();
    page_to_frame_map_->UnsafeInsert(page_id, frame);
    UnlockMapMutex();

    cumulative_misses_time_ns_ +=
        (std::chrono::steady_clock::now() - start).count();
    ++num_misses_;
  }

  frame->Lock(exclusive);

  return *frame;
}

// Unfixes a page updating whether it is dirty or not.
void BufferManager::UnfixPage(BufferFrame& frame, const bool is_dirty) {
  if (is_dirty) frame.SetDirty();

  // Since this page is now unfixed, check if it can be considered for eviction.
  LockEvictionMutex();
  if (frame.DecFixCount() == 0) page_eviction_strategy_->Insert(&frame);
  UnlockEvictionMutex();

  frame.Unlock();
}

// Flushes a page to disk and then unfixes it (the page is not necessarily
// immediately evicted from the cache).
void BufferManager::FlushAndUnfixPage(BufferFrame& frame) {
  WritePageOut(&frame);
  UnfixPage(frame, /*is_dirty=*/false);
}

// Writes all dirty pages to disk (without unfixing).
void BufferManager::FlushDirty() { FlushDirty(/* also_delete = */ false); }

// Writes all dirty pages to disk (without unfixing). If `also_delete` is set,
// all frames are also deleted (used for destructor).
void BufferManager::FlushDirty(const bool also_delete) {
  LockMapMutex();

  for (auto& it : *page_to_frame_map_) {
    auto frame = it.value;
    if (frame->IsDirty()) {
      frame->Lock(/*exclusive=*/false);
      WritePageOut(frame);
      frame->UnsetDirty();
      frame->Unlock();
    }
    if (also_delete) delete frame;
  }

  UnlockMapMutex();
}

// Indicates whether the page given by `page_id` is currently in the buffer
// manager.
bool BufferManager::Contains(const PhysicalPageId page_id) {
  BufferFrame* value_out;

  LockMapMutex();
  bool return_val = page_to_frame_map_->UnsafeLookup(page_id, &value_out);
  UnlockMapMutex();

  return return_val;
}

// Provide the buffer manager with `num_pages` additional cache pages.
void BufferManager::IncreaseCachePages(size_t num_pages) {
  LockEvictionMutex();
  for (size_t i = 0; i < num_pages; ++i) {
    page_eviction_strategy_->Insert(new BufferFrame());
  }
  UnlockEvictionMutex();
  buffer_manager_size_ += num_pages;
}

// Reduce the available cache pages by up to `num_pages`. Returns the actual
// number by which the cache pages were reduced, which might be lower if
// `num_pages` exceeded the number of currently unfixed frames.
size_t BufferManager::ReduceCachePages(size_t num_pages) {
  std::vector<BufferFrame*> evicted;

  LockMapMutex();
  LockEvictionMutex();

  for (size_t i = 0; i < num_pages; ++i) {
    auto frame = page_eviction_strategy_->Evict();
    if (frame == nullptr) break;

    page_to_frame_map_->UnsafeErase(frame->GetPageId());
    --buffer_manager_size_;
    evicted.push_back(frame);
  }

  UnlockEvictionMutex();
  UnlockMapMutex();

  // Write out evicted pages if necessary
  for (auto& frame : evicted) {
    if (frame->IsDirty()) {
      WritePageOut(frame);
    }

    delete frame;
  }

  return evicted.size();
}

// Provides the average latency of a buffer manager miss, in nanoseconds.
std::chrono::nanoseconds BufferManager::BufMgrMissLatency() const {
  if (num_misses_ == 0) {
    return std::chrono::nanoseconds(0);
  } else {
    return std::chrono::nanoseconds(cumulative_misses_time_ns_ / num_misses_);
  }
}

// Writes the page held by `frame` to disk.
void BufferManager::WritePageOut(BufferFrame* frame) const {
  if (file_manager_ == nullptr) return;
  Status s = file_manager_->WritePage(frame->GetPageId(), frame->GetData());
  if (!s.ok()) throw std::runtime_error("Tried to write to unallocated page.");
}

// Reads a page from disk into `frame`.
void BufferManager::ReadPageIn(BufferFrame* frame) {
  if (file_manager_ == nullptr) return;
  Status s = file_manager_->ReadPage(frame->GetPageId(), frame->GetData());
  if (!s.ok()) throw std::runtime_error("Tried to read from unallocated page.");
}

}  // namespace llsm
