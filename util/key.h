#pragma once

#include <cstring>
#include <vector>

#include "treeline/options.h"
#include "treeline/slice.h"

namespace tl {
namespace key_utils {

// Returns the data pointed to by `p` as if it is a pointer to type `T`.
//
// The caller should ensure that the size of the buffer pointed to by `p` is at
// least as large as `sizeof(T)`.
//
// Acknowledgement: This function was originally written by Viktor Leis.
template <class T>
static T LoadUnaligned(const void* p) {
  T x;
  memcpy(&x, p, sizeof(T));
  return x;
}

// Extracts a 4 byte order-preserving prefix of a given key.
//
// This function assumes that keys are ordered lexicographically and that the
// system is little endian. Its purpose is to extract a prefix that can be used
// for fast comparisons.
//
// Acknowledgement: This function was originally written by Viktor Leis.
static uint32_t ExtractHead(const uint8_t* key, unsigned key_length) {
  switch (key_length) {
    case 0:
      return 0;
    case 1:
      return static_cast<uint32_t>(key[0]) << 24;
    case 2:
      return static_cast<uint32_t>(
                 __builtin_bswap16(LoadUnaligned<uint16_t>(key)))
             << 16;
    case 3:
      return (static_cast<uint32_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key)))
              << 16) |
             (static_cast<uint32_t>(key[2]) << 8);
    default:
      return __builtin_bswap32(LoadUnaligned<uint32_t>(key));
  }
}

// Extracts an 8-byte order-preserving prefix of a given key.
//
// This function assumes that keys are ordered lexicographically and that the
// system is little endian. Its purpose is to extract a prefix that can be used
// for fast comparisons.
template <class SliceKind>
static KeyHead ExtractHead64(const SliceKind& key) {
  switch (key.size()) {
    case 0:
      return 0;
    case 1:
      return static_cast<uint64_t>(key.data()[0]) << 56;
    case 2:
      return static_cast<uint64_t>(
                 __builtin_bswap16(LoadUnaligned<uint16_t>(key.data())))
             << 48;
    case 3:
      return (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data())))
              << 48) |
             (static_cast<uint64_t>(key.data()[2]) << 40);
    case 4:
      return static_cast<uint64_t>(
                 __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
             << 32;

    case 5:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(key.data()[4]) << 24);

    case 6:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data() + 4)))
              << 16);
    case 7:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data() + 4)))
              << 16) |
             (static_cast<uint64_t>(key.data()[6]) << 8);
    default:
      return __builtin_bswap64(LoadUnaligned<uint64_t>(key.data()));
  }
}

template <typename T>
std::vector<T> CreateValues(const KeyDistHints& key_hints) {
  std::vector<T> values;
  if (key_hints.num_keys == 0) return values;
  values.reserve(key_hints.num_keys);
  const size_t max_value =
      key_hints.min_key + (key_hints.num_keys - 1) * key_hints.key_step_size;
  for (size_t value = key_hints.min_key; value <= max_value;
       value += key_hints.key_step_size) {
    values.push_back(__builtin_bswap64(value));
  }
  return values;
}

template <typename T>
std::vector<std::pair<Slice, Slice>> CreateRecords(
    const std::vector<T>& values) {
  std::vector<std::pair<Slice, Slice>> records;

  for (uint64_t i = 0; i < values.size(); ++i) {
    const Slice slice = Slice(reinterpret_cast<const char*>(&values.at(i)), 8);
    records.push_back(std::make_pair(slice, slice));
  }

  return records;
}

// Converts 64-bit unsigned integer keys to a byte representation that can be
// compared lexicographically while still maintaining the same sort order. In
// practice, this means the bytes in the key need to be swapped (assuming we're
// running on a little endian machine).
class IntKeyAsSlice {
 public:
  IntKeyAsSlice(uint64_t key) : swapped_(__builtin_bswap64(key)) {}

  template <class SliceKind>
  SliceKind as() const {
    return SliceKind(reinterpret_cast<const char*>(&swapped_),
                     sizeof(swapped_));
  }

 private:
  uint64_t swapped_;
};

}  // namespace key_utils
}  // namespace tl
