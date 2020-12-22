#include "file_manager.h"


namespace llsm {

// Creates a file manager according to the options specified in `options`.
FileManager::FileManager(const BufMgrOptions options, std::string db_path)
    : db_path_(std::move(db_path)),
      pages_per_segment_(options.pages_per_segment_) {
  for (size_t i = 0; i < options.num_files; i++) {
    db_files_.push_back(std::make_unique<File>(
        options, db_path_ + "/segment-" + std::to_string(i)));
  }
}

// Reads the part of the on-disk database file corresponding to `page_id` into
// the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const uint64_t page_id, void* data) {
  size_t segment_id = page_id / pages_per_segment_;
  auto& file = db_files_[segment_id];
  size_t offset = (page_id % pages_per_segment_) * file->GetPageSize();

  file->ZeroOut(offset);
  file->ReadPage(offset, data);
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `page_id`.
void FileManager::WritePage(const uint64_t page_id, void* data) {
  size_t segment_id = page_id / pages_per_segment_;
  auto& file = db_files_[segment_id];
  size_t offset = (page_id % pages_per_segment_) * file->GetPageSize();

  file->ZeroOut(offset);
  file->WritePage(offset, data);
}

}  // namespace llsm
