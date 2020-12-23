#include "buffer_manager.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <mutex>

#include "file_manager.h"
#include "twoqueue_eviction.h"

namespace llsm {

// Initializes a BufferManager to keep up to `buffer_manager_size` frames in main
// memory. Bypasses file system cache if `use_direct_io` is true.
BufferManager::BufferManager(const BufMgrOptions options, std::string db_path)
    : options_(options),
      buffer_manager_size_(options.buffer_manager_size),
      page_size_(options.page_size) {

  // Allocate space with alignment because of O_DIRECT requirements.
  // No need to zero out because data brought here will come from the database
  // `File`, which is zeroed out upon expansion.
  pages_cache_ =
      aligned_alloc(options.alignment, buffer_manager_size_ * page_size_);

  char* page_ptr = reinterpret_cast<char*>(pages_cache_);
  for (size_t i = 0; i < buffer_manager_size_; ++i, page_ptr += page_size_) {
    free_pages_.push_back((void*)page_ptr);
  }
  page_to_frame_map_.reserve(buffer_manager_size_);

  page_eviction_strategy_ = new TwoQueueEviction(buffer_manager_size_);
  file_manager_ = new FileManager(options_, db_path);
}

// Writes all dirty pages back and frees resources.
BufferManager::~BufferManager() {
  LockMapMutex();
  LockEvictionMutex();

  // Write all the dirty pages back.
  BufferFrame* frame;
  while ((frame = page_eviction_strategy_->Evict()) != nullptr) {
    frame->Lock(/*exclusive=*/false);
    if (frame->IsDirty()) WritePageOut(frame);
    frame->Unlock();
  }

  // Delete space for in-memory frames.
  for (auto pair : page_to_frame_map_) {
    if (pair.second != nullptr) {
      delete (pair.second);
    }
  }

  free(pages_cache_);

  delete page_eviction_strategy_;
  delete file_manager_;

  UnlockEvictionMutex();
  UnlockMapMutex();
}

// Retrieves the page given by `page_id`, to be held exclusively or not
// based on the value of `exclusive`. Pages are stored on disk in files with
// the same name as the page ID (e.g. 1).
BufferFrame& BufferManager::FixPage(const uint64_t page_id,
                                    const bool exclusive) {
  BufferFrame* frame;

  // Check if page is already loaded in some frame.
  LockMapMutex();
  LockEvictionMutex();
  auto frame_lookup = page_to_frame_map_.find(page_id);

  // If yes, load the corresponding frame
  if (frame_lookup != page_to_frame_map_.end()) {
    frame = frame_lookup->second;
    if (frame->IncFixCount() == 1) page_eviction_strategy_->Delete(frame);
    UnlockEvictionMutex();
    UnlockMapMutex();

  } else {  // If not, we have to bring it in from disk.
    UnlockEvictionMutex();
    UnlockMapMutex();

    frame = CreateFrame(page_id);

    if (frame == nullptr) {  // Must evict something to make space.
      // Block here until you can evict something
      while (frame == nullptr) {
        LockMapMutex();
        LockEvictionMutex();
        frame = page_eviction_strategy_->Evict();
        if (frame != nullptr) {
          page_to_frame_map_.erase(frame->GetPageId());
        }
        UnlockEvictionMutex();
        UnlockMapMutex();
      }

      // Write out evicted page if necessary
      if (frame->IsDirty()) {
        WritePageOut(frame);
        frame->UnsetDirty();
      }

      // Reset frame to new page_id
      ResetFrame(frame, page_id);
    }

    // Read the page from disk into the selected frame.
    frame->IncFixCount();
    ReadPageIn(frame);

    // Insert the frame into the map.
    LockMapMutex();
    page_to_frame_map_.insert({page_id, frame});
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

// Writes all dirty pages to disk (without unfixing)
void BufferManager::FlushDirty() {
  LockMapMutex();

  for (const auto& pair : page_to_frame_map_) {
    auto frame = pair.second;
    if (frame->IsDirty()) {
      frame->Lock(/*exclusive=*/false);
      WritePageOut(frame);
      frame->UnsetDirty();
      frame->Unlock();
    }
  }

  UnlockMapMutex();
}

// Writes the page held by `frame` to disk.
void BufferManager::WritePageOut(BufferFrame* frame) const {
  file_manager_->WritePage(frame->GetPageId(), frame->GetData());
}

// Reads a page from disk into `frame`.
void BufferManager::ReadPageIn(BufferFrame* frame) {
  file_manager_->ReadPage(frame->GetPageId(), frame->GetData());
}

// Creates a new frame and specifies that it will hold the page with `page_id`.
// Returns nullptr if no new frame can be created.
BufferFrame* BufferManager::CreateFrame(const uint64_t page_id) {
  LockFreePagesMutex();
  if (free_pages_.empty()) {
    UnlockFreePagesMutex();
    return nullptr;
  }
  void* data = free_pages_.front();
  free_pages_.pop_front();
  UnlockFreePagesMutex();

  BufferFrame* frame = new BufferFrame(page_id, data);

  return frame;
}

// Resets an exisiting frame to hold the page with `new_page_id`.
void BufferManager::ResetFrame(BufferFrame* frame, const uint64_t new_page_id) {
  frame->UnsetAllFlags();
  frame->SetPageId(new_page_id);
  frame->ClearFixCount();
}
}  // namespace llsm
