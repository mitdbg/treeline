#include "data.h"

#include <algorithm>
#include <random>
#include <stdexcept>

#include "util/random.h"

namespace tl {
namespace bench {

U64Dataset::U64Dataset(std::vector<uint64_t> keys,
                       std::unique_ptr<uint8_t[]> values, size_t value_size)
    : keys_(std::move(keys)),
      values_(std::move(values)),
      value_size_(value_size),
      records_() {
  records_.reserve(keys_.size());
  for (size_t i = 0; i < keys_.size(); ++i) {
    records_.emplace_back(U64Dataset::Record(
        Slice(reinterpret_cast<const char*>(&keys_[i]), sizeof(uint64_t)),
        Slice(reinterpret_cast<const char*>(&values_[i * value_size_]),
              value_size_)));
  }
}

const U64Dataset::Record& U64Dataset::at(size_t index) const {
  if (index >= size()) {
    throw std::out_of_range("U64Dataset index out of bounds: " +
                            std::to_string(index));
  }
  return operator[](index);
}

U64Dataset U64Dataset::Generate(size_t size_mib, const U64Dataset::GenerateOptions& options) {
  if (options.step_size == 0) {
    throw std::invalid_argument("Step size cannot be 0.");
  }
  if (options.record_size < 9) {
    throw std::invalid_argument(
        "The record size must be at least 9 bytes (8 byte key, 1 byte value).");
  }

  const size_t total_dataset_size = size_mib * 1024 * 1024;
  if (total_dataset_size % options.record_size != 0) {
    throw std::invalid_argument(
        "The requested dataset size is not divisible by the record size.");
  }
  const size_t num_records = total_dataset_size / options.record_size;
  const size_t value_size = options.record_size - sizeof(uint64_t);

  // Generate all the keys
  std::vector<uint64_t> keys;
  keys.reserve(num_records);
  uint64_t next_key = options.start_key;
  for (size_t i = 0; i < num_records; ++i, next_key += options.step_size) {
    if (next_key < options.start_key) {
      // We've wrapped around
      throw std::invalid_argument(
          "Cannot generate `size_mib` of data starting at `start_key` using "
          "`step_size` (not enough `uint64_t` values to represent each "
          "record).");
    }
    // We need to swap the bytes on a little endian machine to ensure
    // lexicographic ordering.
    keys.emplace_back(__builtin_bswap64(next_key));
  }

  // Generate all the values at once
  const size_t total_value_size = value_size * num_records;
  std::unique_ptr<uint8_t[]> values(new uint8_t[total_value_size]);

  // Initialize the values to a deterministic sequence. Zeroing out the values
  // is not ideal because it may make the data too easily compressible.
  Random rng(options.rng_seed);
  const size_t num_of_u32s = total_value_size / sizeof(uint32_t);
  uint32_t* value_ptr = reinterpret_cast<uint32_t*>(values.get());
  for (size_t i = 0; i < num_of_u32s; ++i, ++value_ptr) {
    *value_ptr = rng.Next();
  }

  if (options.shuffle) {
    std::mt19937 shuffle_rng(options.rng_seed);
    std::shuffle(keys.begin(), keys.end(), shuffle_rng);
  }

  return U64Dataset(std::move(keys), std::move(values), value_size);
}

}  // namespace bench
}  // namespace tl
