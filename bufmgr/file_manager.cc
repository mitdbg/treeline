#include "file_manager.h"

#include <cassert>

namespace llsm {

// Creates a file manager according to the options specified in `options`.
FileManager::FileManager(const BufMgrOptions& options,
                         std::filesystem::path db_path)
    : db_path_(std::move(db_path)),
      total_pages_(options.num_pages),
      total_segments_(options.num_segments) {
  // Get number of segments.
  assert(total_segments_ >= 1);

  // Calculate the page-to-segment ratio
  size_t pages_per_segment = total_pages_ / total_segments_;
  if (total_pages_ % total_segments_ != 0) ++pages_per_segment;

  // Create the db_files
  for (size_t i = 0; i < total_segments_; ++i) {
    db_files_.push_back(std::make_unique<File>(
        db_path_ / ("segment-" + std::to_string(i)), options.use_direct_io));
  }

  // Initialize the page table
  FileAddress::SetBitWidths(total_segments_);
  for (size_t i = 0; i < total_pages_; ++i) {
    page_table_.emplace_back(i / pages_per_segment, i % pages_per_segment);
  }
}

// Reads the part of the on-disk database file corresponding to `page_id` into
// the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const uint64_t page_id, void* data) {
  assert(page_id < GetNumPages());
  const FileAddress& address = page_table_[page_id];
  const auto& file = db_files_[address.GetFileId()];
  const size_t byte_offset = address.GetOffset() * Page::kSize;
  file->ExpandToIfNeeded(byte_offset);
  file->ReadPage(byte_offset, data);
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `page_id`.
void FileManager::WritePage(const uint64_t page_id, void* data) {
  assert(page_id < GetNumPages());
  const FileAddress& address = page_table_[page_id];
  const auto& file = db_files_[address.GetFileId()];
  const size_t byte_offset = address.GetOffset() * Page::kSize;
  file->ExpandToIfNeeded(byte_offset);
  file->WritePage(byte_offset, data);
}

}  // namespace llsm
