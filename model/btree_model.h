#pragma once

#include "model.h"
#include "tlx/btree_map.h"
#include "util/tracking_allocator.h"

namespace llsm {

// A model that uses a TLX B+ Tree to predict the location of keys inside the
// database.
class BTreeModel : public Model {
 public:

  BTreeModel();

  // Uses the model to predict a page_id given a `key` that is within the
  // correct range (lower bounds `key`).
  PhysicalPageId KeyToPageId(const Slice& key);
  PhysicalPageId KeyToPageId(const uint64_t key);

  // Uses the model to predict the page_id of the NEXT page given a `key` that
  // is within the correct range (upper bounds `key`). Returns an invalid
  // page_id if no next page exists.
  PhysicalPageId KeyToNextPageId(const Slice& key);
  PhysicalPageId KeyToNextPageId(const uint64_t key);

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
      uint64_t, PhysicalPageId, std::less<uint64_t>,
      tlx::btree_default_traits<uint64_t, std::pair<uint64_t, PhysicalPageId>>,
      TrackingAllocator<std::pair<uint64_t, PhysicalPageId>>>
      index_;
  uint64_t currently_allocated_bytes_ = 0;
  std::shared_mutex mutex_;
};

}  // namespace llsm
