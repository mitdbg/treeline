#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bufmgr/buffer_manager.h"
#include "treeline/slice.h"
#include "util/key.h"

namespace tl {

// A model to determine the correct page for a database record based on the key.
class Model {
 public:
  virtual ~Model() = default;

  // Uses the model to derive a page_id given a `key`. Optionally also returns
  // the 8-byte prefix of the smallest key that maps to the same page.
  virtual PhysicalPageId KeyToPageId(
      const Slice& key, key_utils::KeyHead* base_key_prefix = nullptr) = 0;
  virtual PhysicalPageId KeyToPageId(
      const key_utils::KeyHead key,
      key_utils::KeyHead* base_key_prefix = nullptr) = 0;

  // Uses the model to predict the page_id of the NEXT page given a `key` that
  // is within the correct range (upper bounds `key`). Returns an invalid
  // page_id if no next page exists. Optionally also returns
  // the 8-byte prefix of the smallest key that maps to the same page.
  virtual PhysicalPageId KeyToNextPageId(
      const Slice& key, key_utils::KeyHead* base_key_prefix = nullptr) = 0;
  virtual PhysicalPageId KeyToNextPageId(
      const key_utils::KeyHead key,
      key_utils::KeyHead* base_key_prefix = nullptr) = 0;

  // Inserts a new mapping into the model (updates the page_id if the key
  // already exists).
  virtual void Insert(const Slice& key, const PhysicalPageId& page_id) = 0;

  // Removes a mapping from the model, if the key exists.
  virtual void Remove(const Slice& key) = 0;

  // Gets the number of pages indexed by the model.
  virtual size_t GetNumPages() = 0;

  // Gets the total memory footprint of the model in bytes.
  virtual size_t GetSizeBytes() = 0;

  // Initializes the model based on new files, allocated according to `records`
  // and `records_per_page` and accessed through `buf_mgr`.
  void PreallocateAndInitialize(
      const std::shared_ptr<BufferManager>& buf_mgr,
      const std::vector<std::pair<Slice, Slice>>& records,
      const size_t records_per_page);

  // Initalizes the model based on existing files, accessed through `buf_mgr`.
  void ScanFilesAndInitialize(const std::shared_ptr<BufferManager>& buf_mgr);
};

}  // namespace tl
