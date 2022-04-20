#include "btree_model.h"

#include <limits>

#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace tl {

BTreeModel::BTreeModel()
    : index_(TrackingAllocator<std::pair<key_utils::KeyHead, PhysicalPageId>>(
          currently_allocated_bytes_)) {}

// Uses the model to predict a page_id given a `key` that is within the
// correct range (lower bounds `key`).
PhysicalPageId BTreeModel::KeyToPageId(const Slice& key,
                                       key_utils::KeyHead* base_key_prefix) {
  return BTreeModel::KeyToPageId(key_utils::ExtractHead64(key),
                                 base_key_prefix);
}

PhysicalPageId BTreeModel::KeyToPageId(const key_utils::KeyHead key,
                                       key_utils::KeyHead* base_key_prefix) {
  mutex_.lock_shared();
  auto it = index_.upper_bound(key);
  --it;
  auto page_id = it->second;
  if (base_key_prefix != nullptr) {
    // WARNING: HERE WE ASSUME KEYS OF AT LEAST 8 BYTES.
    *base_key_prefix = it->first;
  }
  mutex_.unlock_shared();
  return page_id;
}

// Uses the model to predict the page_id of the NEXT page given a `key` that
// is within the correct range (upper bounds `key`). Returns an invalid
// page_id if no next page exists.
PhysicalPageId BTreeModel::KeyToNextPageId(
    const Slice& key, key_utils::KeyHead* base_key_prefix) {
  return BTreeModel::KeyToNextPageId(key_utils::ExtractHead64(key),
                                     base_key_prefix);
}

PhysicalPageId BTreeModel::KeyToNextPageId(
    const key_utils::KeyHead key, key_utils::KeyHead* base_key_prefix) {
  mutex_.lock_shared();
  auto it = index_.upper_bound(key);
  PhysicalPageId page_id;
  if (it != index_.end()) {
    page_id = it->second;
    if (base_key_prefix != nullptr) {
      // WARNING: HERE WE ASSUME KEYS OF AT LEAST 8 BYTES.
      *base_key_prefix = it->first;
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
void BTreeModel::Insert(const Slice& key, const PhysicalPageId& page_id) {
  mutex_.lock();
  index_.insert2(key_utils::ExtractHead64(key), page_id);
  mutex_.unlock();
}

// Removes a mapping from the model, if the key exists.
void BTreeModel::Remove(const Slice& key) {
  mutex_.lock();
  index_.erase(key_utils::ExtractHead64(key));
  mutex_.unlock();
}

// Gets the number of pages indexed by the model
size_t BTreeModel::GetNumPages() {
  mutex_.lock_shared();
  const size_t num_pages = index_.size();
  mutex_.unlock_shared();
  return num_pages;
}

// Gets the total memory footprint of the model in bytes.
size_t BTreeModel::GetSizeBytes() {
  mutex_.lock_shared();
  const size_t size_bytes = currently_allocated_bytes_ + sizeof(this->index_);
  mutex_.unlock_shared();
  return size_bytes;
}

}  // namespace tl
