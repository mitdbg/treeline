#include "data.h"

#include <stdexcept>

namespace llsm {
namespace bench {

U64Dataset GenerateOrderedData(size_t size_mib, uint64_t start_key,
                               uint64_t step_size) {
  if (step_size == 0) {
    throw std::invalid_argument("Step size cannot be 0.");
  }
  const size_t num_records = size_mib * 1024 * 1024 / sizeof(U64Record);
  std::vector<U64Record> records;
  records.reserve(num_records);
  uint64_t next_key = start_key;
  for (size_t i = 0; i < num_records; ++i, next_key += step_size) {
    if (next_key < start_key) {
      // We've wrapped around
      throw std::invalid_argument(
          "Cannot generate `size_mib` of data starting at `start_key` using "
          "`step_size` (not enough `uint64_t` values to represent each "
          "record).");
    }
    // We need to swap the bytes on a little endian machine to ensure
    // lexicographic ordering. We use the original key as the value for
    // convenience.
    records.emplace_back(
        U64Record(__builtin_bswap64(next_key), /* value */ next_key));
  }
  return records;
}

}  // namespace bench
}  // namespace llsm
