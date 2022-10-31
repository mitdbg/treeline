#pragma once

#include "model.h"
#include "third_party/tlx/btree_map.h"
#include "util/tracking_allocator.h"

namespace tl {

// A model that uses a TLX B+ Tree to predict the location of keys inside the
// database.
class BTreeModel : public Model {
 public:
  BTreeModel();

  // Uses the model to predict a page_id given a `key` that is within the
  // correct range (lower bounds `key`). Optionally also returns
  // the 8-byte prefix of the smallest key that maps to the same page.
  PhysicalPageId KeyToPageId(const Slice& key,
                             key_utils::KeyHead* base_key_prefix = nullptr);
  PhysicalPageId KeyToPageId(const key_utils::KeyHead key,
                             key_utils::KeyHead* base_key_prefix = nullptr);

  // Uses the model to predict the page_id of the NEXT page given a `key` that
  // is within the correct range (upper bounds `key`). Returns an invalid
  // page_id if no next page exists. Optionally also returns
  // the 8-byte prefix of the smallest key that maps to the same page.
  PhysicalPageId KeyToNextPageId(const Slice& key,
                                 key_utils::KeyHead* base_key_prefix = nullptr);
  PhysicalPageId KeyToNextPageId(const key_utils::KeyHead key,
                                 key_utils::KeyHead* base_key_prefix = nullptr);

  // Inserts a new mapping into the model (updates the page_id if the key
  // already exists).
  void Insert(const Slice& key, const PhysicalPageId& page_id);

  // Removes a mapping from the model, if the key exists.
  void Remove(const Slice& key);

  // Gets the number of pages indexed by the model
  size_t GetNumPages();

  // Gets the total memory footprint of the model in bytes.
  size_t GetSizeBytes();

 private:
  tlx::btree_map<
      key_utils::KeyHead, PhysicalPageId, std::less<key_utils::KeyHead>,
      tlx::btree_default_traits<key_utils::KeyHead,
                                std::pair<key_utils::KeyHead, PhysicalPageId>>,
      TrackingAllocator<std::pair<key_utils::KeyHead, PhysicalPageId>>>
      index_;
  uint64_t currently_allocated_bytes_ = 0;
  std::shared_mutex mutex_;
};

}  // namespace tl
