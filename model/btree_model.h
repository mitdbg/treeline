#pragma once

#include <memory>
#include <vector>

#include "bufmgr/buffer_manager.h"
#include "llsm/options.h"
#include "model.h"
#include "tlx/btree_map.h"

namespace llsm {

// A model that uses a TLX B+ Tree to predict the location of keys inside the
// database.
class BTreeModel : public Model {
 public:
  // Initalizes the model based on a vector of records sorted by key.
  BTreeModel(const KeyDistHints& key_hints,
             const std::vector<std::pair<Slice, Slice>>& records);

  // Initalizes the model based on existing files, accessed through the
  // `buf_mgr`.
  BTreeModel(const std::unique_ptr<BufferManager>& buf_mgr);

  // Preallocates the number of pages deemed necessary after initialization.
  void Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                   const std::unique_ptr<BufferManager>& buf_mgr);

  // Uses the model to predict a page_id given a `key` that is within the
  // correct range (lower bounds `key`).
  PhysicalPageId KeyToPageId(const Slice& key);
  PhysicalPageId KeyToPageId(const uint64_t key);

  // Uses the model to predict the page_id of the NEXT page given a `key` that
  // is within the correct range (upper bounds `key`). Returns an invalid
  // page_id if no next page exists.
  PhysicalPageId KeyToNextPageId(const Slice& key);
  PhysicalPageId KeyToNextPageId(const uint64_t key);

 private:
  tlx::btree_map<uint64_t, PhysicalPageId> index_;
  const size_t records_per_page_;
};

}  // namespace llsm
