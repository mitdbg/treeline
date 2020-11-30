#pragma once

#include "llsm/slice.h"
#include "llsm/status.h"
#include "util/packed_map.h"

namespace llsm {

// An in-memory representation of LLSM's on-disk "page" format.
//
// This class is a thin wrapper that helps with manipulating the contents of a
// page when it is loaded into memory. A page is the "unit" of storage we use to
// actually store the keys and values inserted into an LLSM database.
class Page {
 public:
  Page() = default;

  Status Put(const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value_out);
  Status Delete(const Slice& key);

  Status LoadFrom(const Slice& buf);
  void EncodeTo(std::string& dest) const;

 private:
  static constexpr size_t kSize = 4096;
  PackedMap<kSize> rep_;
  static_assert(sizeof(rep_) == kSize);
};

}  // namespace llsm
