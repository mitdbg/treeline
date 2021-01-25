#include "file_manager.h"

#include <cassert>

namespace llsm {

// Creates a file manager according to the options specified in `options`.
FileManager::FileManager(const Options options, std::string db_path)
    : db_path_(std::move(db_path)) {
  // Get number of segments.
  const size_t segments = options.background_threads;
  assert(segments >= 1);

  // Compute the number of pages needed.
  total_pages_ = options.num_keys / options.records_per_page;
  if (options.num_keys % options.records_per_page != 0) ++total_pages_;

  // Compute the page allocation
  size_t pages_per_segment = total_pages_ / segments;
  if (total_pages_ % segments != 0) ++pages_per_segment;
  for (size_t i = 0, j = 0; i < segments; ++i, j += pages_per_segment)
    page_allocation_.push_back(j);

  // Create the db_files
  for (size_t i = 0; i < segments; ++i) {
    db_files_.push_back(std::make_unique<File>(
        options, db_path_ + "/segment-" + std::to_string(i)));
  }
}

// Reads the part of the on-disk database file corresponding to `page_id` into
// the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const uint64_t page_id, void* data) {
  const FileAddress address = PageIdToAddress(page_id);
  const auto& file = db_files_[address.file_id];
  file->ZeroOut(address.offset);
  file->ReadPage(address.offset, data);
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `page_id`.
void FileManager::WritePage(const uint64_t page_id, void* data) {
  const FileAddress address = PageIdToAddress(page_id);
  const auto& file = db_files_[address.file_id];
  file->ZeroOut(address.offset);
  file->WritePage(address.offset, data);
}

// Uses the model to derive a FileAddress given a `page_id`.
//
// Deriving the right segment currently takes time logarithmic in the number of
// segments, which is probably efficient enough for a small number of
// segments.
FileAddress FileManager::PageIdToAddress(const size_t page_id) const {
  assert(page_id < GetNumPages());

  // Find file_id
  const size_t file_id = std::upper_bound(page_allocation_.begin(),
                                          page_allocation_.end(), page_id) -
                         1 - page_allocation_.begin();

  // Find offset
  const size_t offset =
      (page_id - page_allocation_.at(file_id)) * Page::kSize;
  return FileAddress{file_id, offset};
}

}  // namespace llsm
