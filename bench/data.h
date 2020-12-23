#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "llsm/slice.h"

namespace llsm {
namespace bench {

// An immutable collection of key-value records where each key is a 64-bit
// unsigned integer. Values have a configurable size, but all values in a
// dataset will have the same size.
class U64Dataset {
 public:
  // Generates a dataset containing ordered synthetic records with a total size
  // of `size_mib`.
  //
  // Callers must ensure that `size_mib` (in bytes) is divisible by
  // `record_size`. Otherwise, this method will throw an `std::illegal_argument`
  // exception. Note that `record_size` allows you to indirectly set the value
  // size; the key size is always 8 bytes.
  //
  // The record keys are in ascending order when compared lexicographically. The
  // data generation code assumes that the system is little endian.
  //
  // Set the `start_key` and `step_size` to configure the start key and the size
  // of the difference between successive keys. The generated keys are uniformly
  // distributed.
  static U64Dataset GenerateOrdered(size_t size_mib, size_t record_size = 16,
                                    uint64_t start_key = 0,
                                    uint64_t step_size = 1);

  class Record;
  // Retrieve the record at `index` with bounds checking.
  const Record& at(size_t index) const;
  // Retrieve the record at `index` without bounds checking.
  const Record& operator[](size_t index) const { return records_[index]; }

  using const_iterator = std::vector<Record>::const_iterator;
  const_iterator begin() const { return records_.begin(); }
  const_iterator end() const { return records_.end(); }

  // Number of records in this dataset.
  size_t size() const { return records_.size(); }

  // The size, in bytes, of the value in a record in this dataset.
  size_t value_size() const { return value_size_; }

 private:
  U64Dataset(std::vector<uint64_t> keys, std::unique_ptr<uint8_t[]> values,
             size_t value_size);

  const std::vector<uint64_t> keys_;
  // All values are stored contiguously
  const std::unique_ptr<uint8_t[]> values_;
  // The size of a single value
  const size_t value_size_;
  // A materialized list of the records in this dataset
  std::vector<Record> records_;
};

// A thin wrapper that represents a single `U64Dataset` record. The underlying
// `U64Dataset` must remain valid for the lifetime of this record.
class U64Dataset::Record {
 public:
  const Slice& key() const { return key_; }
  const Slice& value() const { return value_; }

 private:
  friend class U64Dataset;
  Record(Slice key, Slice value)
      : key_(std::move(key)), value_(std::move(value)) {}

  const Slice key_;
  const Slice value_;
};

}  // namespace bench
}  // namespace llsm
