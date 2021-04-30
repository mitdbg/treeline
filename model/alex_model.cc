#include "alex_model.h"

#include <limits>

#include "bufmgr/page_memory_allocator.h"
#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Uses the model to predict a page_id given a `key` that is within the
// correct range (lower bounds `key`).
PhysicalPageId ALEXModel::KeyToPageId(const Slice& key) {
  return ALEXModel::KeyToPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId ALEXModel::KeyToPageId(const uint64_t key) {
  mutex_.lock_shared();
  PhysicalPageId* page_id_ptr = index_.get_payload_last_no_greater_than(key);
  PhysicalPageId page_id;
  if (page_id_ptr != nullptr) {
    page_id = *page_id_ptr;
  }
  mutex_.unlock_shared();
  return page_id;
}

// Uses the model to predict the page_id of the NEXT page given a `key` that
// is within the correct range (upper bounds `key`). Returns an invalid
// page_id if no next page exists.
PhysicalPageId ALEXModel::KeyToNextPageId(const Slice& key) {
  return ALEXModel::KeyToNextPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId ALEXModel::KeyToNextPageId(const uint64_t key) {
  mutex_.lock_shared();
  PhysicalPageId* page_id_ptr = index_.get_payload_upper_bound(key);
  PhysicalPageId page_id;
  if (page_id_ptr != nullptr) {
    page_id = *page_id_ptr;
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
