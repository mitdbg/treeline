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
  size_t KeyToPageId(const Slice& key) const;
  size_t KeyToPageId(const uint64_t key) const;

 private:
  rs::RadixSpline<uint64_t> index_;
  const size_t records_per_page_;
};

}  // namespace llsm
