#pragma once

#include <string>

#include "segment_id.h"
#include "../plr/data.h"
#include "treeline/options.h"
#include "treeline/slice.h"
#include "treeline/status.h"

namespace tl {
namespace pg {

// An in-memory representation of TreeLine's on-disk "page" format.
//
// This class is a thin wrapper that helps with manipulating the contents of a
// page when it is loaded into memory. A page is the "unit" of storage we use to
// actually store the keys and values inserted into an TreeLine database.
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
  static constexpr size_t kSize = 4 * 1024;

  // The number of bytes in a `Page` that can be used to store data. This value
  // depends on `Page::kSize` and will be smaller than it (some space is used
  // for internal bookkeeping).
  static size_t UsableSize();

  // The number of bytes of extra metadata used per record. Each record
  // currently uses additional metadata for the "slots" abstraction, which takes
  // space in the "usable" portion of the page.
  static size_t PerRecordMetadataSize();

  // The number of records of size `record_size` that would fit into
  // an empty page with `total_fence_bytes` already used for fences.
  // `record_size` should already take into account any compression because of
  // common prefixes.
  static size_t NumRecordsThatFit(size_t record_size, size_t total_fence_bytes);

  // The minimum number of empty pages, with `total_fence_bytes` already used
  // for fences in each page, that are required to hold `n` records of size
  // `record_size`. `record_size` should already take into account any
  // compression because of common prefixes.
  static size_t NumPagesNeeded(size_t n, size_t record_size,
                               size_t total_fence_bytes);

  // Construct a `Page` that refers to its contents in the `data` buffer.
  explicit Page(void* data) : data_(data) {}

  // Construct an empty `Page` where all keys will satisfy
  // `lower_key <= key <= upper_key` (used for efficient encoding of the keys).
  // The `Page`'s contents will be stored in the `data` buffer.
  Page(void* data, const Slice& lower_key, const Slice& upper_key);

  // Construct an overflow `Page` with the same key boundaries as `old_page`.
  // The `Page`'s contents will be stored in the `data` buffer.
  Page(void* data, const Page& old_page);

  // All keys stored in this page have this prefix. Note that the returned
  // `Slice` shares the same lifetime as this page's `data` buffer.
  Slice GetKeyPrefix() const;

  // Get the key boundaries for this page.
  Slice GetLowerBoundary() const;
  Slice GetUpperBoundary() const;

  Status Put(const Slice& key, const Slice& value);
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
  Status UpdateOrRemove(const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value_out);
  Status Delete(const Slice& key);

  // Check whether this is a valid Page (as opposed to a Page-sized
  // block of 0s).
  const bool IsValid() const;

  // Returns the number of records stored in this page.
  uint16_t GetNumRecords() const;

  // Check whether this is an overflow page & make/unmake it one.
  const bool IsOverflow() const;
  void MakeOverflow();
  void UnmakeOverflow();

  // Retrieve the stored `overflow` page id for this page.
  SegmentId GetOverflow() const;

  // Set the stored overflow page id for this page to `overflow`.
  void SetOverflow(SegmentId overflow);

  // Determine whether this page has an overflow page.
  bool HasOverflow() const;

  // Retrieve/update the model associated with this page.
  // VALID FOR: First page in a multi-page segment only.
  plr::Line64 GetModel() const;
  void SetModel(const plr::Line64& model);

  // Retrieve/update the segment's sequence number.
  // VALID FOR: First (only) page in a single-page segment.
  // VALID FOR: Second page in a segment (multi-page segments).
  uint32_t GetSequenceNumber() const;
  void SetSequenceNumber(uint32_t sequence);

  // Retrieve/update the segment's checksum.
  // VALID FOR: Second page in a multi-page segment only.
  uint32_t GetChecksum() const;
  void SetChecksum(uint32_t checksum);

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
  // Construct an empty `Page` where all keys will satisfy
  // `lower_key <= key <= upper_key` (used for efficient encoding of the keys).
  // The `Page`'s contents will be stored in the `data` buffer.
  Page(void* data, const uint8_t* lower_key, unsigned lower_key_length,
       const uint8_t* upper_key, unsigned upper_key_length);

  void* data_;
};

// An iterator used to iterate over the records stored in this page. Use
// `Page::GetIterator()` to get an instance.
//
// NOTE: The underlying memory buffer that backs the `Page` that created this
// iterator must remain valid for the lifetime of this iterator. Usually this
// just means that the page must remain fixed by the buffer manager for the
// lifetime of this iterator.
class Page::Iterator {
 public:
  // Move the iterator to the first record with a key that is greater than or
  // equal to `key`. If such a record exists, `Valid()` will return true after
  // this method returns.
  void Seek(const Slice& key);

  // Move the iterator to the last record in the page. As long as the page is
  // non-empty, `Valid()` will return true after this method returns.
  void SeekToLast();

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

 private:
  friend class Page;
  explicit Iterator(const Page& page);

  // Points to the this iterator's page's underlying memory buffer.
  const void* data_;
  size_t current_slot_;

  // Keys in the page are stored using a prefix (shared by all keys) and a
  // suffix (specific to each record). To reconstruct the record's actual key,
  // we need to concatenate the prefix with the suffix. The members below are
  // used to lazily reconstruct the record's key if `key()` is called.
  size_t prefix_length_;
  mutable bool key_buffer_valid_;
  mutable std::string key_buffer_;
};

}  // namespace pg
}  // namespace tl
