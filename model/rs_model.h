#pragma once

#include <memory>
#include <vector>

#include "../rs/builder.h"
#include "bufmgr/buffer_manager.h"
#include "llsm/options.h"
#include "model.h"

namespace llsm {

// A model that uses RadixSpline to predict the location of keys inside the
// database.
class RSModel : public Model {
 public:
  // Initalizes the model based on a vector of records sorted by key.
  RSModel(const KeyDistHints& key_hints,
          const std::vector<std::pair<Slice, Slice>>& records);

  // Preallocates the number of pages deemed necessary after initialization.
  void Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                   const std::unique_ptr<BufferManager>& buf_mgr);

  // Uses the model to predict a page_id given a `key` that is within the
  // correct range.
  LogicalPageId KeyToPageId(const Slice& key) const;
  LogicalPageId KeyToPageId(const uint64_t key) const;

  // Serializes the model and appends it to `dest`.
  void EncodeTo(std::string* dest) const override;

  // Parses a `RSModel` from `source` and advances `source` past the parsed
  // bytes.
  //
  // If the parse is successful, `status_out` will be OK and the returned
  // pointer will be non-null. If the parse is unsuccessful, `status_out` will
  // be non-OK and the returned pointer will be `nullptr`.
  static std::unique_ptr<Model> LoadFrom(Slice* source, Status* status_out);

 private:
  // Creates the `RSModel` by providing values for its members directly (used
  // when deserializing the model).
  RSModel(const rs::RadixSpline<uint64_t> index, const size_t records_per_page);

  rs::RadixSpline<uint64_t> index_;
  const size_t records_per_page_;
};

}  // namespace llsm
