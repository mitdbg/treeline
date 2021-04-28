#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "bufmgr/physical_page_id.h"

namespace llsm {

class Slice;
class Status;

// A model to determine the correct page for a database record based on the key.
class Model {
 public:
  virtual ~Model() = default;

  // Uses the model to derive a page_id given a `key`.
  virtual PhysicalPageId KeyToPageId(const Slice& key) = 0;

  // Uses the model to predict the page_id of the NEXT page given a `key` that
  // is within the correct range (upper bounds `key`). Returns an invalid
  // page_id if no next page exists.
  virtual PhysicalPageId KeyToNextPageId(const Slice& key) = 0;

  // Inserts a new mapping into the model (updates the page_id if the key
  // already exists).
  virtual void Insert(const Slice& key, const PhysicalPageId& page_id) = 0;

  // Removes a mapping from the model, if the key exists.
  virtual void Remove(const Slice& key) = 0;

  // Gets the number of pages indexed by the model
  virtual size_t GetNumPages() const = 0;
};

}  // namespace llsm
