#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <random>
#include <unordered_set>

#include "../common.h"
#include "zipf_distribution.h"

// Reads in Wikipedia edit timestamp dataset and outputs key-value pairs of the
// form <`user_id`><`timestamp`> / <`value`> where `user_id` are Zipfian
// distributed user ids, `timestamp` is the edit timestamp, and `value` is a
// meaningless integer. All components are 64-bit unsigned integers. Ensures
// that output is unique (does not contain any duplicates).

constexpr size_t kNumUniqueUserIds = 65536;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: ./genwiki <filename>" << std::endl;
    exit(EXIT_FAILURE);
  }

  const std::string filename = argv[1];
  const size_t num_bits_user_id = std::ceil(log2(kNumUniqueUserIds));
  std::cout << "Number of bits for user id: " << num_bits_user_id << std::endl;
  std::cout << "Number of bits for timestamp: " << 64 - num_bits_user_id
            << std::endl;

  // Load Wikipedia edit timestamps.
  std::vector<uint64_t> timestamps = load_data<uint64_t>(filename);

  // Remove precision. We clear the most-significant bits since clearing the
  // least-significant ones leads to many duplicates. Initially, there are
  // 90437011 unique values (45%). After this modification, there are still that
  // many unique values. We could even use 32-bit user ids and this wouldn't
  // change, meaning the timestamps share at least a 32-bit prefix. These 32
  // bits are not zero though (there are at most two leading zeros).
  for (int i = 0; i < timestamps.size(); ++i) {
    timestamps[i] = (timestamps[i] << num_bits_user_id) >> num_bits_user_id;
  }

  // Remove duplicates.
  std::unordered_set<uint64_t> set;
  set.reserve(timestamps.size());
  for (int i = 0; i < timestamps.size(); ++i) {
    set.insert(timestamps[i]);
  }

  // Copy back to vector.
  timestamps.clear();
  timestamps.insert(timestamps.end(), set.begin(), set.end());
  std::sort(timestamps.begin(), timestamps.end());

  std::cout << "Number of unique timestamps: " << timestamps.size()
            << std::endl;

  // Generate trace.
  std::vector<Record> records;
  records.reserve(timestamps.size());

  std::mt19937 gen(42);
  // Generates numbers [1, kNumUniqueUserIds].
  zipf_distribution<uint64_t> zipf(kNumUniqueUserIds, /*q=*/1.1);
  // `zipf_distribution` generates numbers such that 1 is the hottest. We use
  // this vector to make a different number the hottest etc.
  std::vector<uint64_t> zipf_shuffle(kNumUniqueUserIds);
  std::iota(zipf_shuffle.begin(), zipf_shuffle.end(), 0);
  std::shuffle(zipf_shuffle.begin(), zipf_shuffle.end(), gen);

  for (uint64_t i = 0; i < timestamps.size(); ++i) {
    uint64_t key = timestamps[i];

    // Prepend user id.
    const uint64_t idx = zipf(gen) - 1;  // `- 1` to make numbers start from 0.
    uint64_t user_id = zipf_shuffle[idx];
    user_id = user_id << (64 - num_bits_user_id);
    key = key | user_id;

    records.push_back({key, i});
  }

  // Write records to CSV file.
  const std::string out_filename = filename + "_trace_keys.csv";
  write_to_csv(records, out_filename, /*key_only=*/true);

  return 0;
}
