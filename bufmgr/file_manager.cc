#include "file_manager.h"

#include <cassert>

namespace llsm {

// Creates a file manager according to the options specified in `options`.
FileManager::FileManager(const BufMgrOptions& options,
                         std::filesystem::path db_path)
    : db_path_(std::move(db_path)),
      total_pages_(options.num_pages),
      total_segments_(options.num_segments),
      next_segment_to_expand_(0) {
  // Get number of segments.
  assert(total_segments_ >= 1);

  // Calculate the page-to-segment ratio. If not exact, last segment will hold
  // fewer pages.
  size_t pages_per_normal_segment = total_pages_ / total_segments_;
  size_t pages_for_last_segment;
  if (total_pages_ % total_segments_ == 0) {
    pages_for_last_segment = pages_per_normal_segment;
  } else {
    ++pages_per_normal_segment;
    pages_for_last_segment =
        total_pages_ - (pages_per_normal_segment * (total_segments_ - 1));
  }

  // Create the db_files
  for (size_t i = 0; i < total_segments_ - 1; ++i) {
    db_files_.push_back(std::make_unique<File>(
        db_path_ / ("segment-" + std::to_string(i)), options.use_direct_io,
        pages_per_normal_segment));
  }
  db_files_.push_back(std::make_unique<File>(
      db_path_ / ("segment-" + std::to_string(total_segments_ - 1)),
      options.use_direct_io, pages_for_last_segment));

  // Initialize the page table
  PhysicalPageId::SetBitWidths(total_segments_);
  for (size_t i = 0; i < total_pages_; ++i) {
    page_table_.emplace_back(i / pages_per_normal_segment,
                             i % pages_per_normal_segment);
  }
}

// Reads the part of the on-disk database file corresponding to
// `logical_page_id` into the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const LogicalPageId logical_page_id, void* data) {
  const PhysicalPageId& physical_page_id = GetPhysicalPageId(logical_page_id);
  const auto& file = db_files_[physical_page_id.GetFileId()];
  const size_t byte_offset = physical_page_id.GetOffset() * Page::kSize;
  file->ReadPage(byte_offset, data);
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `logical_page_id`.
void FileManager::WritePage(const LogicalPageId logical_page_id, void* data) {
  const PhysicalPageId& physical_page_id = GetPhysicalPageId(logical_page_id);
  const auto& file = db_files_[physical_page_id.GetFileId()];
  const size_t byte_offset = physical_page_id.GetOffset() * Page::kSize;
  file->WritePage(byte_offset, data);
}

// Allocates a new page as an overflow page and updates the overflow page
// table accordingly. Returns the page id of the newly allocated page.
LogicalPageId FileManager::AllocatePage() {
  page_allocation_mutex_.lock();
  // Pick which file to expand
  const size_t file_id = next_segment_to_expand_++;
  if (next_segment_to_expand_ >= total_segments_) {
    next_segment_to_expand_ = 0;
  }

  // Allocate the page
  const auto& file = db_files_[file_id];
  const size_t offset = file->AllocatePage();

  // Update overflow page table
  overflow_page_table_.emplace_back(file_id, offset / Page::kSize);
  uint64_t raw_page_id = overflow_page_table_.size() - 1;
  page_allocation_mutex_.unlock();
  return LogicalPageId(raw_page_id, /*is_overflow = */ true);
}

// Consult the correct page table to retrieve the PhysicalPageId for
// `logical_page_id`.
PhysicalPageId& FileManager::GetPhysicalPageId(const LogicalPageId logical_page_id) {
  size_t raw_page_id = logical_page_id.GetRawPageId();
  if (logical_page_id.IsOverflow()) {
    assert(raw_page_id < overflow_page_table_.size());
    return overflow_page_table_[raw_page_id];
  } else {
    assert(raw_page_id < page_table_.size());
    return page_table_[raw_page_id];
  }
}

}  // namespace llsm
