#include "memtable.h"

namespace llsm {

Status MemTable::Put(const Slice& key, const Slice& value) {
  return InsertImpl(key, value, MemTable::EntryType::kWrite);
}

Status MemTable::Get(const Slice& key, EntryType* entry_type_out,
                     std::string* value_out) const {
  const auto it = table_.find(key);
  if (it == table_.end()) {
    return Status::NotFound("Requested key was not found:", key);
  }
  const EntryType entry_type = it->second.second;
  if (entry_type == EntryType::kWrite) {
    const Slice& value = it->second.first;
    value_out->clear();
    value_out->append(value.data(), value.size());
  }
  *entry_type_out = entry_type;
  return Status::OK();
}

Status MemTable::Delete(const Slice& key) {
  return InsertImpl(key, Slice(), MemTable::EntryType::kDelete);
}

Status MemTable::InsertImpl(const Slice& key, const Slice& value,
                            MemTable::EntryType entry_type) {
  // This implementation assumes that re-inserts or insert-then-deletes
  // are rare, so we always allocate new space for the key and value.
  size_t bytes = key.size() + value.size();
  char* buf = arena_.Allocate(bytes);
  memcpy(buf, key.data(), key.size());
  memcpy(buf + key.size(), value.data(), value.size());

  const Slice owned_key(buf, key.size());
  const Slice owned_value =
      value.empty() ? Slice() : Slice(buf + key.size(), value.size());

  // We overwrite any existing value (if it exists)
  Value& val = table_[owned_key];
  val.first = owned_value;
  val.second = entry_type;

  return Status::OK();
}

}  // namespace llsm
