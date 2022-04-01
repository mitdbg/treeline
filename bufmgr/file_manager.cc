#include "file_manager.h"

#include <cassert>

namespace tl {

FileManager::FileManager(const BufMgrOptions& options,
                         std::filesystem::path db_path)
    : db_path_(std::move(db_path)),
      total_pages_(0),
      total_segments_(options.num_segments),
      next_segment_to_expand_(0) {
  // Get number of segments and initialize the page ids
  assert(total_segments_ >= 1);
  PhysicalPageId::SetBitWidths(total_segments_);

  // Create the db_files
  for (size_t i = 0; i < total_segments_; ++i) {
    db_files_.push_back(std::make_unique<File>(
        db_path_ / ("segment-" + std::to_string(i)), options.use_direct_io, 0));
  }
}

Status FileManager::ReadPage(const PhysicalPageId physical_page_id, void* data) {
  const auto& file = db_files_[physical_page_id.GetFileId()];
  const size_t byte_offset = physical_page_id.GetOffset() * Page::kSize;
  return file->ReadPage(byte_offset, data);
}

Status FileManager::WritePage(const PhysicalPageId physical_page_id, void* data) {
  const auto& file = db_files_[physical_page_id.GetFileId()];
  const size_t byte_offset = physical_page_id.GetOffset() * Page::kSize;
  return file->WritePage(byte_offset, data);
}

PhysicalPageId FileManager::AllocatePage() {
  page_allocation_mutex_.lock();
  // Pick which file to expand
  const size_t file_id = next_segment_to_expand_++;
  if (next_segment_to_expand_ >= total_segments_) {
    next_segment_to_expand_ = 0;
  }

  // Allocate the page
  const auto& file = db_files_[file_id];
  const size_t offset = file->AllocatePage();

  ++total_pages_;
  page_allocation_mutex_.unlock();
  return PhysicalPageId(file_id, offset / Page::kSize);
}

}  // namespace tl
