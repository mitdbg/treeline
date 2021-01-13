#pragma once

#include <cstring>

#include "llsm/options.h"

namespace llsm {
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
static uint64_t ExtractHead64(const Slice& key) {
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
std::vector<T> CreateValues(const Options options) {
  std::vector<T> values;
  values.reserve(options.num_keys);
  for (size_t i = 0; i < options.num_keys; i += options.key_step_size)
    values.push_back(__builtin_bswap64(i));
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

}  // namespace key_utils
}  // namespace llsm
