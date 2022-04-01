#pragma once

#include "../alex/alex.h"
#include "model.h"

namespace tl {

// A model that uses ALEX to predict the location of keys inside the
// database.
class ALEXModel : public Model {
 public:
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
  alex::Alex<key_utils::KeyHead, PhysicalPageId, alex::AlexCompare,
             std::allocator<std::pair<key_utils::KeyHead, PhysicalPageId>>,
             /* allow_duplicates = */ false>
      index_;
  std::shared_mutex mutex_;
};

}  // namespace tl
