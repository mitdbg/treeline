#include "direct_model.h"

#include <limits>

#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Preallocates the number of pages deemed necessary after initialization.
void DirectModel::Preallocate(const std::unique_ptr<BufferManager>& buf_mgr) {
  // Preallocate the pages with the key space
  const uint64_t page_key_range = records_per_page_ * key_step_size_;
  const uint64_t total_pages = buf_mgr->GetFileManager()->GetNumPages();
  uint64_t lower_key = 0;
  uint64_t upper_key = page_key_range;
  for (unsigned page_id = 0; page_id < total_pages; ++page_id) {
    const uint64_t swapped_lower = __builtin_bswap64(lower_key);
    const uint64_t swapped_upper = __builtin_bswap64(upper_key);
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    Page page(bf.GetData(),
              Slice(reinterpret_cast<const char*>(&swapped_lower), 8),
              Slice(reinterpret_cast<const char*>(&swapped_upper), 8));
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);

    lower_key += page_key_range;
    upper_key += page_key_range;
  }
  buf_mgr->FlushDirty();
};

// Uses the model to derive a page_id given a `key` that is within the correct
// range.
size_t DirectModel::KeyToPageId(const Slice& key) const {
  return KeyToPageId(key_utils::ExtractHead64(key));
}

size_t DirectModel::KeyToPageId(const uint64_t key) const {
  const uint64_t unit_step_key = key / key_step_size_;
  return unit_step_key / records_per_page_;
}

void DirectModel::EncodeTo(std::string* dest) const {
  // Format:
  // - Model type identifier  (1 byte)
  // - Records per page       (uint64; 8 bytes)
  // - Key step size          (uint64; 8 bytes)
  dest->push_back(static_cast<uint8_t>(detail::ModelType::kDirectModel));
  assert(records_per_page_ <= std::numeric_limits<uint64_t>::max());
  assert(key_step_size_ <= std::numeric_limits<uint64_t>::max());
  PutFixed64(dest, records_per_page_);
  PutFixed64(dest, key_step_size_);
}

std::unique_ptr<Model> DirectModel::LoadFrom(Slice* source, Status* status_out) {
  if (source->size() < (1 + 8 + 8)) {
    *status_out = Status::InvalidArgument("Not enough bytes to deserialize a DirectModel.");
    return nullptr;
  }

  const uint8_t raw_model_type = (*source)[0];
  if (raw_model_type != static_cast<uint8_t>(detail::ModelType::kDirectModel)) {
    *status_out = Status::InvalidArgument("Attempted to deserialize a non-DirectModel.");
    return nullptr;
  }
  source->remove_prefix(1);

  const uint64_t records_per_page = DecodeFixed64(source->data());
  source->remove_prefix(8);
  const uint64_t key_step_size = DecodeFixed64(source->data());
  source->remove_prefix(8);

  *status_out = Status::OK();
  return std::unique_ptr<Model>(new DirectModel(records_per_page, key_step_size));
}

}  // namespace llsm
