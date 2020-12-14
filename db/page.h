#pragma once

#include "llsm/slice.h"
#include "llsm/status.h"

namespace llsm {

// An in-memory representation of LLSM's on-disk "page" format.
//
// This class is a thin wrapper that helps with manipulating the contents of a
// page when it is loaded into memory. A page is the "unit" of storage we use to
// actually store the keys and values inserted into an LLSM database.
//
// A `Page` object does not actually own the buffer where it stores its data. As
// a result, users must ensure that the `data` buffer used to construct a `Page` is
// (i) kept valid for the lifetime of the `Page`, and (ii) that its size is at
// least as large as `Page::kSize`. Not meeting these constraints leads to
// undefined behavior.
//
// This class is not thread-safe; external mutual exclusion is required.
class Page {
 public:
  static constexpr size_t kSize = 64 * 1024;

  // Construct a `Page` that refers to its contents in the `data` buffer.
  explicit Page(void* data) : data_(data) {}

  // Construct an empty `Page` where all keys will satisfy
  // `lower_key <= key <= upper_key` (used for efficient encoding of the keys).
  // The `Page`'s contents will be stored in the `data` buffer.
  Page(void* data, const Slice& lower_key, const Slice& upper_key);

  Status Put(const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value_out);
  Status Delete(const Slice& key);

  // Returns a `Slice` (a read-only view) of this `Page`'s underlying raw
  // representation.
  Slice data() const {
    return Slice(reinterpret_cast<const char*>(data_), kSize);
  }

 private:
  void* data_;
};

}  // namespace llsm
