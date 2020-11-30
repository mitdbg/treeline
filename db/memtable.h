#pragma once

#include <map>

#include "llsm/slice.h"
#include "llsm/status.h"
#include "util/arena.h"

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
// This class is not thread-safe; external mutual exclusion is required.
class MemTable {
 public:
  enum class EntryType {
    kWrite = 0,
    kDelete = 1
  };

  MemTable() = default;
  Status Put(const Slice& key, const Slice& value);
  Status Get(const Slice& key, EntryType* entry_type_out,
             std::string* value_out) const;
  Status Delete(const Slice& key);

  using Key = Slice;
  using Value = std::pair<Slice, EntryType>;

 private:
  class Comparator {
   public:
    bool operator()(const Key& k1, const Key& k2) const {
      return k1.compare(k2) < 0;
    }
  };
  using Table = std::map<Key, Value, Comparator>;

 public:
  using iterator = Table::iterator;
  using const_iterator = Table::const_iterator;

  iterator begin() { return table_.begin(); }
  iterator end() { return table_.end(); }
  const_iterator begin() const { return table_.begin(); }
  const_iterator end() const { return table_.end(); }

 private:
  // A helper method used to implement Put() and Delete(), which are both
  // considered `MemTable` "inserts" (see comments at the top of this class).
  Status InsertImpl(const Slice& key, const Slice& value,
                    MemTable::EntryType entry_type);

  // A custom memory-managed arena that stores the actual keys and values.
  Arena arena_;
  Table table_;
};

}  // namespace llsm
