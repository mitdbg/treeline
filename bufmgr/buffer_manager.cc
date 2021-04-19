#include "buffer_manager.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/statvfs.h>

#include <mutex>

#include "file_manager.h"
#include "twoqueue_eviction.h"

namespace llsm {

// Initializes a BufferManager to keep up to `options.buffer_pool_size /
// Page::kSize` frames in main memory. Bypasses file system cache if
// `options.use_direct_io` is true.
BufferManager::BufferManager(const BufMgrOptions& options,
                             std::filesystem::path db_path)
    : buffer_manager_size_(options.buffer_pool_size / Page::kSize) {
  size_t alignment = BufferManager::kDefaultAlignment;
  struct statvfs fs_stats;
  if (statvfs(db_path.c_str(), &fs_stats) == 0) {
    alignment = fs_stats.f_bsize;
  }
  // Allocate space with alignment because of O_DIRECT requirements.
  pages_cache_ = aligned_alloc(alignment, options.buffer_pool_size);
  memset(pages_cache_, 0, options.buffer_pool_size);

  page_to_frame_map_ =
      std::make_unique<SyncHashTable<PhysicalPageId, BufferFrame*>>(
          buffer_manager_size_, /*num_partitions = */ 1);
  frames_ = std::vector<BufferFrame>(buffer_manager_size_);
  SetFrameDataPointers();
  free_ptr_ = 0;
  no_free_left_ = false;

  page_eviction_strategy_ =
      std::make_unique<TwoQueueEviction>(buffer_manager_size_);
  if (!options.simulation_mode) {
    file_manager_ = std::make_unique<FileManager>(options, std::move(db_path));
  } else {
    file_manager_ = nullptr;
  }
}

// Writes all dirty pages back and frees resources.
BufferManager::~BufferManager() {
  FlushDirty();
  free(pages_cache_);
}

// Retrieves the page given by `page_id`, to be held exclusively or not
// based on the value of `exclusive`. Pages are stored on disk in files with
// the same name as the page ID (e.g. 1).
BufferFrame& BufferManager::FixPage(const PhysicalPageId page_id,
                                    const bool exclusive) {
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

    frame = GetFreeFrame();

    if (frame == nullptr) {  // Must evict something to make space.
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
    }

    // Initialize and populate the frame
    frame->Initialize(page_id);
    frame->IncFixCount();
    ReadPageIn(frame);

    // Insert the frame into the map.
    LockMapMutex();
    page_to_frame_map_->UnsafeInsert(page_id, frame);
    UnlockMapMutex();
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

// Writes all dirty pages to disk (without unfixing)
void BufferManager::FlushDirty() {
  LockMapMutex();

  for (auto& frame : frames_) {
    if (frame.IsDirty()) {
      frame.Lock(/*exclusive=*/false);
      WritePageOut(&frame);
      frame.UnsetDirty();
      frame.Unlock();
    }
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

// If there are free frames left, returns one of them. Else, returns nullptr.
BufferFrame* BufferManager::GetFreeFrame() {
  if (no_free_left_) return nullptr;

  size_t frame_id_loc = free_ptr_++;
  if (frame_id_loc >= buffer_manager_size_) {
    no_free_left_ = true;
    return nullptr;
  } else {
    return &frames_[frame_id_loc];
  }
}

void BufferManager::SetFrameDataPointers() {
  char* page_ptr = reinterpret_cast<char*>(pages_cache_);

  for (uint64_t frame_id = 0; frame_id < frames_.size(); ++frame_id) {
    frames_[frame_id].SetData(reinterpret_cast<void*>(page_ptr));
    page_ptr += Page::kSize;
  }
}

}  // namespace llsm
