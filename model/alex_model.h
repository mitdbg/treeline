#pragma once

#include "../alex/alex.h"
#include "model.h"

namespace llsm {

// A model that uses ALEX to predict the location of keys inside the
// database.
class ALEXModel : public Model {
 public:
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
  alex::Alex<uint64_t, PhysicalPageId, alex::AlexCompare,
             std::allocator<std::pair<uint64_t, PhysicalPageId>>,
             /* allow_duplicates = */ false>
      index_;
  std::shared_mutex mutex_;
};

}  // namespace llsm
