#pragma once

#include "llsm/slice.h"

namespace llsm {

class BufferManager;

// A model to determine the correct page for a database record based on the key.
class Model {
 public:
  // Uses the model to derive a page_id given a `key`.
  virtual size_t KeyToPageId(const Slice& key) const = 0;
};

}  // namespace llsm