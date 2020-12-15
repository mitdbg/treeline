#pragma once

#include "llsm/slice.h"
#include "llsm/status.h"
#include "util/arena.h"
#include "util/skiplist.h"

namespace llsm {

// An ordered in-memory table storing "recent" writes and deletes.
//
// This `MemTable` explicitly stores deletes, distinguishing them from regular
// writes using `EntryType::kDelete`. This is important because the key may
// still exist on disk, and we need to know that it should be deleted when this
// `MemTable` is flushed to disk.
//
// Data is stored in a custom memory-managed arena. Once this `MemTable` is done
// being used, it should be deleted to free its allocated memory.
//
// The `Put()` and `Delete()` methods are not thread-safe; external mutual
// exclusion is required when calling these methods.
//
// `Get()` or `GetIterator()` (including the Iterator methods) are thread-safe
// and can be used without external mutual exclusion. This `MemTable` must
// remain valid while any `Get()` calls are being executed and while any
// `Iterator`s are being used.
class MemTable {
 public:
  enum class EntryType : uint8_t { kWrite = 0, kDelete = 1 };

  MemTable();
  Status Put(const Slice& key, const Slice& value);
  Status Get(const Slice& key, EntryType* entry_type_out,
             std::string* value_out) const;
  Status Delete(const Slice& key);

  class Iterator;
  Iterator GetIterator() const;

  size_t ApproximateMemoryUsage() const;

 private:
  struct Record {
    Record();
    Record(const char* data, uint32_t key_length, uint32_t value_length,
           uint32_t key_head, uint64_t sequence_number);
    // Key and value are stored contiguously
    const char* const data;
    const uint32_t key_length;
    const uint32_t value_length;
    // We store the first 4 bytes of the key inside the skip list node to
    // hopefully avoid the need to dereference `data` in most cases.
    const uint32_t key_head;
    // The sequence number is used to de-duplicate `Record`s with the same
    // `key`. If multiple `Record`s share the same key, the one with the largest
    // sequence number is the most recent entry.
    //
    // The upper 7 bytes store the sequence number (max 2^56 - 1) and the lowest
    // byte stores the `EntryType`.
    const uint64_t sequence_number;
  };
  class Comparator {
   public:
    // Classical comparison semantics. We return a:
    // - Negative integer if `r1 < r2`
    // - Zero if `r1 == r2`
    // - Positive integer if `r1 > r2`
    int operator()(const Record& r1, const Record& r2) const;
  };
  using Table = SkipList<Record, Comparator>;

  // A helper method used to implement Put() and Delete(), which are both
  // considered `MemTable` "inserts" (see comments at the top of this class).
  Status InsertImpl(const Slice& key, const Slice& value,
                    MemTable::EntryType entry_type);

  // A custom memory-managed arena that stores the actual keys and values.
  Arena arena_;
  Table table_;
  uint64_t next_sequence_num_;
};

// An iterator for the MemTable.
//
// To get an instance, call `GetIterator()` on a `MemTable`. One of the Seek
// methods must be called first before `Next()` can be called.
//
// When `Valid()` returns `true`, the `key()`, `value()`, and `type()` methods
// return the key, value, and entry type associated with the record that the
// iterator currently "points" to.
class MemTable::Iterator {
 public:
  // Returns true iff the iterator is positioned at a valid node.
  bool Valid() const;
  // Returns the key at the current position.
  // REQUIRES: `Valid()`
  Slice key() const;
  // Returns the value at the current position.
  // REQUIRES: `Valid()`
  Slice value() const;
  // Returns the entry type at the current position.
  // REQUIRES: `Valid()`
  MemTable::EntryType type() const;

  // Advances to the next position.
  // REQUIRES: Valid()
  void Next();
  // Advance to the first entry with a key >= target
  void Seek(const Slice& target);
  // Position at the first entry in list.
  // Final state of iterator is `Valid()` iff list is not empty.
  void SeekToFirst();

 private:
  friend class MemTable;
  explicit Iterator(Table::Iterator it) : it_(it) {}

  // A helper method that returns a `Record` that can be used to search in the
  // underlying skip list for a given raw `key`.
  MemTable::Record GetLookupKey(const Slice& key) const;

  // A helper method that returns the key stored in an internal record.
  Slice KeyFromRecord(const MemTable::Record& record) const;

  Table::Iterator it_;
};

}  // namespace llsm
