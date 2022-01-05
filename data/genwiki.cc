#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <random>
#include <unordered_set>

#include "common.h"

// Reads in Wikipedia edit timestamp dataset and outputs key-value pairs of the
// form <`user_id`><`timestamp`> / <`value`> where `user_id` are uniformly
// distributed user ids, `timestamp` is the original edit timestamp, and `value`
// is a random integer. All components are 64-bit unsigned integers. Ensures
// that output is unique (does not contain any duplicates).

constexpr size_t kNumUniqueUserIds = 65536;
constexpr size_t kNumBitsUserId = 16;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: ./genwiki <filename>" << std::endl;
    exit(EXIT_FAILURE);
  }

  const std::string filename = argv[1];

  // Load Wikipedia edit timestamps.
  std::vector<uint64_t> timestamps = load_data<uint64_t>(filename);

  // Remove precision. We clear the most-significant bits since clearing the
  // least-significant ones leads to many duplicates. Initially, there are
  // 90437011 unique values (45%). After this modification, there are still that
  // many unique values. We could even use 32-bit user ids and this wouldn't
  // change, meaning the timestamps share at least a 32-bit prefix. These 32
  // bits are not zero though (there are at most two leading zeros).
  for (int i = 0; i < timestamps.size(); ++i) {
    timestamps[i] = (timestamps[i] << kNumBitsUserId) >> kNumBitsUserId;
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

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, kNumUniqueUserIds - 1);

  for (uint64_t i = 0; i < timestamps.size(); ++i) {
    uint64_t key = timestamps[i];

    // Prepend user id.
    uint64_t user_id = dist(gen);
    user_id = user_id << (64 - kNumBitsUserId);
    key = key | user_id;

    records.push_back({key, i});
  }

  // Write records to CSV file.
  const std::string out_filename = filename + "_trace.csv";
  write_to_csv(records, out_filename);

  return 0;
}
