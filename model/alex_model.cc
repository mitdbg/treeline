#include "alex_model.h"

#include <limits>

#include "bufmgr/page_memory_allocator.h"
#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Uses the model to predict a page_id given a `key` that is within the
// correct range (lower bounds `key`).
PhysicalPageId ALEXModel::KeyToPageId(const Slice& key,
                                      key_utils::KeyHead* base_key_prefix) {
  return ALEXModel::KeyToPageId(key_utils::ExtractHead64(key), base_key_prefix);
}

PhysicalPageId ALEXModel::KeyToPageId(const key_utils::KeyHead key,
                                      key_utils::KeyHead* base_key_prefix) {
  mutex_.lock_shared();
  auto it = index_.find_last_no_greater_than(key);
  auto page_id = it.payload();
  if (base_key_prefix != nullptr) {
    // WARNING: HERE WE ASSUME KEYS OF AT LEAST 8 BYTES.
    *base_key_prefix = it.key();
  }
  mutex_.unlock_shared();
  return page_id;
}

// Uses the model to predict the page_id of the NEXT page given a `key` that
// is within the correct range (upper bounds `key`). Returns an invalid
// page_id if no next page exists.
PhysicalPageId ALEXModel::KeyToNextPageId(const Slice& key,
                                          key_utils::KeyHead* base_key_prefix) {
  return ALEXModel::KeyToNextPageId(key_utils::ExtractHead64(key),
                                    base_key_prefix);
}

PhysicalPageId ALEXModel::KeyToNextPageId(const key_utils::KeyHead key,
                                          key_utils::KeyHead* base_key_prefix) {
  mutex_.lock_shared();
  auto it = index_.upper_bound(key);
  PhysicalPageId page_id;
  if (it != index_.end()) {
    page_id = it.payload();
    if (base_key_prefix != nullptr) {
      // WARNING: HERE WE ASSUME KEYS OF AT LEAST 8 BYTES.
      *base_key_prefix = it.key();
    }
  } else {
    if (base_key_prefix != nullptr) {
      // WARNING: HERE WE ASSUME KEYS OF AT LEAST 8 BYTES.
      *base_key_prefix = std::numeric_limits<key_utils::KeyHead>::max();
    }
  }
  mutex_.unlock_shared();
  return page_id;
}

// Inserts a new mapping into the model (updates the page_id if the key
// already exists).
void ALEXModel::Insert(const Slice& key, const PhysicalPageId& page_id) {
  mutex_.lock();
  index_.insert(key_utils::ExtractHead64(key), page_id);
  mutex_.unlock();
}

// Removes a mapping from the model, if the key exists.
void ALEXModel::Remove(const Slice& key) {
  mutex_.lock();
  index_.erase(key_utils::ExtractHead64(key));
  mutex_.unlock();
}

// Gets the number of pages indexed by the model
size_t ALEXModel::GetNumPages() {
  mutex_.lock_shared();
  const size_t num_pages = index_.size();
  mutex_.unlock_shared();
  return num_pages;
}

// Gets the total memory footprint of the model in bytes.
size_t ALEXModel::GetSizeBytes() {
  mutex_.lock_shared();
  const size_t size_bytes = index_.model_size() + index_.data_size();
  mutex_.unlock_shared();
  return size_bytes;
}

}  // namespace llsm
