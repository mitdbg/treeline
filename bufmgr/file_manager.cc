#include "file_manager.h"

#include <cassert>

namespace llsm {

// Creates a file manager according to the options specified in `options`.
FileManager::FileManager(const BufMgrOptions options, std::string db_path)
    : db_path_(std::move(db_path)),
      page_size_(options.page_size),
      // `__builtin_clzl(val)` returns the number of leading zeros in the binary
      // representation of `val`. We create a segment per file, so
      // `pages_per_segment_bits_` must be equal to the number of leading zeros
      // in `options.num_files`.
      pages_per_segment_bits_(__builtin_clzl(options.num_files)),
      pages_per_segment_mask_((1ULL << pages_per_segment_bits_) - 1) {
  assert(options.num_files >= 1);

  for (size_t i = 0; i < options.num_files; ++i) {
    db_files_.push_back(std::make_unique<File>(
        options, db_path_ + "/segment-" + std::to_string(i)));
  }
}

// Reads the part of the on-disk database file corresponding to `page_id` into
// the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const uint64_t page_id, void* data) {
  const SegmentAddress address = AddressFromPageId(page_id);
  const auto& file = db_files_[address.segment_id];
  file->ZeroOut(address.offset);
  file->ReadPage(address.offset, data);
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `page_id`.
void FileManager::WritePage(const uint64_t page_id, void* data) {
  const SegmentAddress address = AddressFromPageId(page_id);
  const auto& file = db_files_[address.segment_id];
  file->ZeroOut(address.offset);
  file->WritePage(address.offset, data);
}

FileManager::SegmentAddress FileManager::AddressFromPageId(
    size_t page_id) const {
  SegmentAddress address;
  address.segment_id = page_id >> pages_per_segment_bits_;
  address.offset = (page_id & pages_per_segment_mask_) * page_size_;
  return address;
}

}  // namespace llsm
