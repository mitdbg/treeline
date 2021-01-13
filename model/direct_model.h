#pragma once

#include <memory>

#include "bufmgr/buffer_manager.h"
#include "llsm/options.h"
#include "model.h"

namespace llsm {

// A direct "model" that assigns a known range of keys (integers from min_key
// inclusive up to max_key exclusive, separated by a fixed step size) to pages
// so as to achieve a target page utilization.
class DirectModel : public Model {
 public:
  // Creates the model based on the provided `options`.
  DirectModel(const Options options)
      : records_per_page_(options.records_per_page),
        key_step_size_(options.key_step_size) {}

  // Preallocates the necessary pages.
  void Preallocate(const std::unique_ptr<BufferManager>&);

  // Uses the model to derive a page_id given a `key` that is within the correct
  // range.
  size_t KeyToPageId(const Slice& key) const;
  size_t KeyToPageId(const uint64_t key) const;

 private:
  const size_t records_per_page_;
  const size_t key_step_size_;
};

}  // namespace llsm
