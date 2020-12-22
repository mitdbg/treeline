#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "llsm/slice.h"

namespace llsm {
namespace bench {

class U64Record {
 public:
  U64Record() : U64Record(0, 0) {}
  U64Record(uint64_t key, uint64_t value) : key_(key), value_(value) {}

  Slice key() const {
    return Slice(reinterpret_cast<const char*>(&key_), sizeof(key_));
  }
  Slice value() const {
    return Slice(reinterpret_cast<const char*>(&value_), sizeof(value_));
  }

 private:
  const uint64_t key_;
  const uint64_t value_;
};

using U64Dataset = std::vector<U64Record>;

// Generates ordered synthetic records with a total size of `size_mib`.
//
// The record keys are in ascending order when compared lexicographically. The
// data generation code assumes that the system is little endian.
//
// Set the `start_key` and `step_size` to configure the start key and the size
// of the difference between successive keys. The generated keys are uniformly
// distributed.
U64Dataset GenerateOrderedData(size_t size_mib, uint64_t start_key = 0,
                               uint64_t step_size = 1);

}  // namespace bench
}  // namespace llsm
