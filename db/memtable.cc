#include "memtable.h"

#include <cassert>
#include <limits>

#include "util/key.h"

namespace {

constexpr uint64_t kWriteTypeMask = 0xFF;
constexpr uint64_t kWriteTypeBitWidth = 8;

}  // namespace

namespace tl {

MemTable::MemTable(const MemTableOptions& moptions)
    : arena_(),
      table_(MemTable::Comparator(), &arena_),
      next_sequence_num_(moptions.deferral_granularity + 1),
      has_entries_(false),
      flush_threshold_(moptions.flush_threshold),
      deferral_granularity_(moptions.deferral_granularity) {}

Status MemTable::Put(const Slice& key, const Slice& value) {
  return Add(key, value, format::WriteType::kWrite);
}

Status MemTable::Get(const Slice& key, format::WriteType* write_type_out,
                     std::string* value_out) const {
  Iterator it = GetIterator();
  it.Seek(key);
  if (!it.Valid() || key.compare(it.key()) != 0) {
    return Status::NotFound("Requested key was not found.");
  }
  value_out->assign(it.value().data(), it.value().size());
  *write_type_out = it.type();
  return Status::OK();
}

Status MemTable::Delete(const Slice& key) {
  return Add(key, Slice(), format::WriteType::kDelete);
}

MemTable::Iterator MemTable::GetIterator() const {
  return MemTable::Iterator(MemTable::Table::Iterator(&table_));
}

size_t MemTable::ApproximateMemoryUsage() const { return arena_.MemoryUsage(); }

Status MemTable::Add(const Slice& key, const Slice& value,
                     format::WriteType write_type, const bool from_deferral,
                     const uint64_t injected_sequence_num) {
  // This implementation assumes that re-inserts or insert-then-deletes
  // are rare, so we always allocate new space for the key and value.
  const size_t user_data_bytes = key.size() + value.size();
  char* buf = table_.AllocateKey(sizeof(Record) + user_data_bytes);

  uint64_t seq_num = 0;
  if (!from_deferral) {
    seq_num = next_sequence_num_++;
  } else {
    // Previous memtables might have had a higher granularity.
    if (injected_sequence_num > deferral_granularity_) {
      seq_num = deferral_granularity_;
    } else {
      seq_num = injected_sequence_num;
    }
  }

  Record* record = Record::FromRawBytes(buf);
  record->key_length = key.size();
  record->value_length = value.size();
  record->sequence_number =
      (seq_num << kWriteTypeBitWidth) | static_cast<uint8_t>(write_type);

  memcpy(record->key(), key.data(), key.size());
  memcpy(record->value(), value.data(), value.size());

  table_.Insert(buf);
  has_entries_ = true;
  return Status::OK();
}

int MemTable::Comparator::operator()(const char* r1_raw,
                                     const Record* r2) const {
  const Record* r1 = Record::FromRawBytes(r1_raw);

  // Check entire keys first.
  const Slice r1_key(r1->key(), r1->key_length),
      r2_key(r2->key(), r2->key_length);
  int comp = r1_key.compare(r2_key);
  if (comp != 0) {
    return comp;
  }

  // Keys are equal, now check the sequence number. Note that this case only
  // occurs if there are "duplicate insertions" (e.g., inserting the same key
  // more than once, doing an insert-then-delete, etc.). To ensure our iterator
  // only reports the "latest" entry for a given key, we order entries with
  // higher sequence numbers first. The underlying skip list sorts in ascending
  // order, so we "negate" the comparator output based on the sequence numbers'
  // relative ordering.
  //
  // We store the write type in the least significant 8 bits of the sequence
  // number. However, since each record has a distinct sequence number (mod
  // 2^56), we can just compare the numbers directly here.
  if (r1->sequence_number < r2->sequence_number) {
    // We report r1 > r2 here so the latest record (r2) is ordered ahead
    return 1;
  } else if (r1->sequence_number > r2->sequence_number) {
    // We report r1 < r2 here so the latest record (r1) is ordered ahead
    return -1;
  } else {
    return 0;
  }
}

bool MemTable::Iterator::Valid() const { return it_.Valid(); }

Slice MemTable::Iterator::key() const {
  assert(Valid());
  return KeyFromRecord(it_.key());
}

Slice MemTable::Iterator::value() const {
  assert(Valid());
  const Record* rec = Record::FromRawBytes(it_.key());
  return Slice(rec->value(), rec->value_length);
}

format::WriteType MemTable::Iterator::type() const {
  assert(Valid());
  const Record* rec = Record::FromRawBytes(it_.key());
  return static_cast<format::WriteType>(rec->sequence_number & kWriteTypeMask);
}

uint64_t MemTable::Iterator::seq_num() const {
  assert(Valid());
  const Record* rec = Record::FromRawBytes(it_.key());
  return (rec->sequence_number >> kWriteTypeBitWidth);
}

void MemTable::Iterator::Next() {
  assert(Valid());
  const Slice last_key = KeyFromRecord(it_.key());

  // There may be duplicate records that share the same key. We order the
  // records so that the latest record always appears first. Therefore the
  // purpose of this loop is to "skip over" any records that have the same key
  // as `last_key`.
  while (true) {
    it_.Next();
    if (!Valid()) {
      break;
    }

    const Slice curr_key = KeyFromRecord(it_.key());
    if (last_key.compare(curr_key) != 0) {
      break;
    }

    // This record uses the same key; it must be an older entry. So we move on
    // to the next record.
  }
}

void MemTable::Iterator::Seek(const Slice& target) {
  char buf[sizeof(Record) + target.size()];
  Record* record = MemTable::Record::FromRawBytes(buf);
  record->key_length = target.size();
  record->value_length = 0;
  // Records with the same key are ordered in descending order based on their
  // sequence number. Since the underlying skip list's iterator's `Seek()`
  // method positions the iterator next to the first record >= the lookup
  // record, we use the maximum sequence number to look up the latest record.
  record->sequence_number = std::numeric_limits<uint64_t>::max();
  memcpy(record->key(), target.data(), target.size());
  it_.Seek(buf);
}

void MemTable::Iterator::SeekToFirst() { it_.SeekToFirst(); }

Slice MemTable::Iterator::KeyFromRecord(const char* raw_record) const {
  const Record* record = Record::FromRawBytes(raw_record);
  return Slice(record->key(), record->key_length);
}

}  // namespace tl
