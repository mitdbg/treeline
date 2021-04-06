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
  // Creates the model based on the provided `key_hints`.
  DirectModel(const KeyDistHints& key_hints)
      : DirectModel(key_hints.records_per_page(), key_hints.key_step_size) {}

  // Preallocates the necessary pages.
  void Preallocate(const std::unique_ptr<BufferManager>&);

  // Uses the model to derive a page_id given a `key` that is within the correct
  // range.
  LogicalPageId KeyToPageId(const Slice& key) const;
  LogicalPageId KeyToPageId(const uint64_t key) const;

  // Serializes the model and appends it to `dest`.
  void EncodeTo(std::string* dest) const override;

  // Parses a `DirectModel` from `source` and advances `source` past the parsed
  // bytes.
  //
  // If the parse is successful, `status_out` will be OK and the returned
  // pointer will be non-null. If the parse is unsuccessful, `status_out` will
  // be non-OK and the returned pointer will be `nullptr`.
  static std::unique_ptr<Model> LoadFrom(Slice* source, Status* status_out);

 private:
  DirectModel(size_t records_per_page, size_t key_step_size)
      : records_per_page_(records_per_page), key_step_size_(key_step_size) {}

  const size_t records_per_page_;
  const size_t key_step_size_;
};

}  // namespace llsm
