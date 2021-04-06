#pragma once

#include <string>

#include "llsm/options.h"
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
// a result, users must ensure that the `data` buffer used to construct a `Page`
// is (i) kept valid for the lifetime of the `Page`, and (ii) that its size is
// at least as large as `Page::kSize`. Not meeting these constraints leads to
// undefined behavior.
//
// This class is not thread-safe; external mutual exclusion is required.
class Page {
 public:
  // The number of bytes needed to store a `Page` (in memory and on disk).
  static constexpr size_t kSize = 64 * 1024;

  // The number of bytes in a `Page` that can be used to store data. This value
  // depends on `Page::kSize` and will be smaller than it (some space is used
  // for internal bookkeeping).
  static constexpr size_t UsableSize();

  // The number of bytes of extra metadata used per record. Each record
  // currently uses additional metadata for the "slots" abstraction, which takes
  // space in the "usable" portion of the page.
  static constexpr size_t PerRecordMetadataSize();

  // Construct a `Page` that refers to its contents in the `data` buffer.
  explicit Page(void* data) : data_(data) {}

  // Construct an empty `Page` where all keys will satisfy
  // `lower_key <= key <= upper_key` (used for efficient encoding of the keys).
  // The `Page`'s contents will be stored in the `data` buffer.
  Page(void* data, const Slice& lower_key, const Slice& upper_key);

  // All keys stored in this page have this prefix. Note that the returned
  // `Slice` shares the same lifetime as this page's `data` buffer.
  Slice GetKeyPrefix() const;

  Status Put(const Slice& key, const Slice& value);
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value_out);
  Status Delete(const Slice& key);

  // Retrieve the stored `overflow` page id for this page.
  uint64_t GetOverflow() const;

  // Set the stored overflow page id for this page to `overflow`.
  void SetOverflow(uint64_t overflow);

  // Determine whether this page has an overflow page. Only page id's with 1 as
  // the most significant bit are valid overflow page id's.
  bool HasOverflow();

  class Iterator;
  friend class Iterator;

  // Returns an iterator that can be used to iterate over the records stored in
  // this page. The returned iterator will initially be positioned at the first
  // record in this page.
  Iterator GetIterator() const;

  // Returns a `Slice` (a read-only view) of this `Page`'s underlying raw
  // representation.
  Slice data() const {
    return Slice(reinterpret_cast<const char*>(data_), kSize);
  }

 private:
  void* data_;
};

// An iterator used to iterate over the records stored in this page. Use
// `Page::GetIterator()` to get an instance.
class Page::Iterator {
 public:
  // Move the iterator to the first record with a key that is greater than or
  // equal to `key`. If such a record exists, `Valid()` will return true after
  // this method returns.
  void Seek(const Slice& key);

  // Move to the next record.
  // REQUIRES: `Valid()` is true.
  void Next();

  // If true, this iterator is currently positioned at a valid record.
  bool Valid() const;

  // Returns the number of remaining records, including the current record, that
  // will be visited by this iterator (e.g., if the iterator is positioned at
  // the second-last record, this method will return 2).
  size_t RecordsLeft() const;

  // Returns the key of the record at the iterator's current position.
  // REQUIRES: `Valid()` is true.
  Slice key() const;

  // Returns the value of the record at the iterator's current position.
  // REQUIRES: `Valid()` is true.
  Slice value() const;

  // Copying is disallowed.
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  // Move construction is allowed.
  Iterator(Iterator&&) = default;

 private:
  friend class Page;
  Iterator(const Page& page);

  const Page& page_;
  size_t current_slot_;

  // Keys in the page are stored using a prefix (shared by all keys) and a
  // suffix (specific to each record). To reconstruct the record's actual key,
  // we need to concatenate the prefix with the suffix. The members below are
  // used to lazily reconstruct the record's key if `key()` is called.
  size_t prefix_length_;
  mutable bool key_buffer_valid_;
  mutable std::string key_buffer_;
};

}  // namespace llsm
