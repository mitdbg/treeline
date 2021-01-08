#pragma once

#include <memory>

#include "llsm/options.h"
#include "model.h"
#include "bufmgr/buffer_manager.h"


namespace llsm {

// A direct "model" that assigns a known range of keys (integers from min_key
// inclusive up to max_key exclusive, separated by a fixed step size) to pages
// so as to achieve a target page utilization.
class DirectModel : public Model {
 public:
  // Creates the model based on the key range and the target page utilization,
  // provided through `options`.
  DirectModel(Options options, BufMgrOptions* buf_mgr_options);

  // Preallocates the necessary pages.
  void Preallocate(const std::unique_ptr<BufferManager>&);

  // Uses the model to derive a page_id given a `key` that is within the correct
  // range.
  size_t KeyToPageId(const Slice& key) const;
  size_t KeyToPageId(const uint64_t key) const;

  // Uses the model to derive a FileAddress given a `page_id`.
  FileAddress PageIdToAddress(const size_t page_id) const;

 private:
  // The database options used to initialize this model.
  Options options_;

  uint32_t total_pages_ = 0;
  uint32_t pages_per_segment_ = 0;
  uint32_t segments_ = 0;
  uint32_t records_per_page_ = 0;
};

}  // namespace llsm
