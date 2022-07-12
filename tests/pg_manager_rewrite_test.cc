#include <algorithm>
#include <filesystem>
#include <numeric>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "page_grouping/key.h"
#include "page_grouping/manager.h"
#include "page_grouping/segment_info.h"
#include "pg_datasets.h"

namespace {

using namespace tl;
using namespace tl::pg;

class PGManagerRewriteTest : public testing::Test {
 public:
  PGManagerRewriteTest()
      : kDBDir("/tmp/tl-pg-test-" + std::to_string(std::time(nullptr))) {}
  void SetUp() override {
    std::filesystem::remove_all(kDBDir);
    std::filesystem::create_directory(kDBDir);
  }
  void TearDown() override { std::filesystem::remove_all(kDBDir); }

  const std::filesystem::path kDBDir;
};

std::vector<std::pair<uint64_t, Slice>> BuildRecords(
    const std::vector<uint64_t>& keys, Slice value) {
  std::vector<std::pair<uint64_t, Slice>> records;
  records.reserve(keys.size());
  for (const auto& key : keys) {
    records.emplace_back(key, value);
  }
  return records;
}

PageGroupedDBOptions GetOptions(size_t goal, size_t epsilon, bool use_segments) {
  PageGroupedDBOptions options;
  options.records_per_page_goal = goal;
  options.records_per_page_epsilon = epsilon;
  options.use_segments = use_segments;
  options.write_debug_info = false;
  options.use_memory_based_io = true;
  options.num_bg_threads = 0;
  return options;
}

TEST_F(PGManagerRewriteTest, AppendSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);
  options.num_bg_threads = 2;

  // Insert sequential records.
  const size_t num_inserts = 100;
  const size_t max_key = Datasets::kUniformKeys.back();
  std::vector<uint64_t> keys_to_insert;
  keys_to_insert.resize(num_inserts);
  std::iota(keys_to_insert.begin(), keys_to_insert.end(), max_key + 10);
  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Create inserts.
  const std::string inserted_value = u8"08+bytes";
  std::vector<std::pair<uint64_t, Slice>> inserts;
  inserts.reserve(keys_to_insert.size());
  for (const auto& key : keys_to_insert) {
    inserts.emplace_back(key, inserted_value);
  }

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Make the inserts.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(max_key + 10, num_inserts + 100, &values);
    ASSERT_EQ(values.size(), num_inserts);
    ASSERT_EQ(values.size(), inserts.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, inserts[i].first);
      ASSERT_EQ(inserts[i].second.compare(values[i].second), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(max_key + 10, num_inserts + 100, &values);
    ASSERT_EQ(values.size(), num_inserts);
    ASSERT_EQ(values.size(), inserts.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, inserts[i].first);
      ASSERT_EQ(inserts[i].second.compare(values[i].second), 0);
    }
  }
}

TEST_F(PGManagerRewriteTest, AppendPages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);
  options.num_bg_threads = 2;

  // Insert sequential records.
  const size_t num_inserts = 100;
  const size_t max_key = Datasets::kUniformKeys.back();
  std::vector<uint64_t> keys_to_insert;
  keys_to_insert.resize(num_inserts);
  std::iota(keys_to_insert.begin(), keys_to_insert.end(), max_key + 10);
  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Create inserts.
  const std::string inserted_value = u8"08+bytes";
  std::vector<std::pair<uint64_t, Slice>> inserts;
  inserts.reserve(keys_to_insert.size());
  for (const auto& key : keys_to_insert) {
    inserts.emplace_back(key, inserted_value);
  }

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Make the inserts.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(max_key + 10, num_inserts + 100, &values);
    ASSERT_EQ(values.size(), num_inserts);
    ASSERT_EQ(values.size(), inserts.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, inserts[i].first);
      ASSERT_EQ(inserts[i].second.compare(values[i].second), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(max_key + 10, num_inserts + 100, &values);
    ASSERT_EQ(values.size(), num_inserts);
    ASSERT_EQ(values.size(), inserts.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, inserts[i].first);
      ASSERT_EQ(inserts[i].second.compare(values[i].second), 0);
    }
  }
}

TEST_F(PGManagerRewriteTest, InsertMiddleSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);
  options.num_bg_threads = 2;

  // Create a new dataset by multiplying each sequential key by 1000.
  std::vector<uint64_t> new_keys;
  new_keys.reserve(Datasets::kSequentialKeys.size());
  for (const auto& k : Datasets::kSequentialKeys) {
    new_keys.push_back(k * 1000);
  }
  const std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(new_keys, u8"08 bytes");

  // Create the insert keys.
  std::mt19937 prng(1337);
  const size_t num_inserts = 800;
  std::vector<uint64_t> keys_to_insert;
  keys_to_insert.reserve(num_inserts);
  const size_t mid = new_keys.size() / 2;
  const std::vector<uint64_t> left = Datasets::FloydSample(
      400, new_keys[mid - 50] + 1, new_keys[mid - 49] - 1, prng);
  const std::vector<uint64_t> right = Datasets::FloydSample(
      400, new_keys[mid + 50] + 1, new_keys[mid + 51] - 1, prng);
  keys_to_insert.insert(keys_to_insert.end(), left.begin(), left.end());
  keys_to_insert.insert(keys_to_insert.end(), right.begin(), right.end());

  // Create insert records.
  const std::string inserted_value = u8"08+bytes";
  std::vector<std::pair<uint64_t, Slice>> inserts;
  inserts.reserve(keys_to_insert.size());
  for (const auto& key : keys_to_insert) {
    inserts.emplace_back(key, inserted_value);
  }
  std::sort(inserts.begin(), inserts.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

  // Expected database contents (for testing).
  std::vector<std::pair<uint64_t, Slice>> all_records;
  all_records.reserve(dataset.size() + inserts.size());
  std::merge(dataset.begin(), dataset.end(), inserts.begin(), inserts.end(),
             std::back_inserter(all_records),
             [](const auto& left, const auto& right) {
               return left.first < right.first;
             });

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Make the inserts.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(1, all_records.size() + 1000000, &values);
    ASSERT_EQ(values.size(), all_records.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, all_records[i].first);
      ASSERT_EQ(all_records[i].second.compare(values[i].second), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(1, all_records.size() + 1000000, &values);
    ASSERT_EQ(values.size(), all_records.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, all_records[i].first);
      ASSERT_EQ(all_records[i].second.compare(values[i].second), 0);
    }
  }
}

TEST_F(PGManagerRewriteTest, InsertMiddlePages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);
  options.num_bg_threads = 2;

  // Create a new dataset by multiplying each sequential key by 1000.
  std::vector<uint64_t> new_keys;
  new_keys.reserve(Datasets::kSequentialKeys.size());
  for (const auto& k : Datasets::kSequentialKeys) {
    new_keys.push_back(k * 1000);
  }
  const std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(new_keys, u8"08 bytes");

  // Create the insert keys.
  std::mt19937 prng(1337);
  const size_t num_inserts = 100;
  std::vector<uint64_t> keys_to_insert;
  keys_to_insert.reserve(num_inserts);
  const size_t mid = new_keys.size() / 2;
  const std::vector<uint64_t> left = Datasets::FloydSample(
      50, new_keys[mid - 50] + 1, new_keys[mid - 49] - 1, prng);
  const std::vector<uint64_t> right = Datasets::FloydSample(
      50, new_keys[mid + 50] + 1, new_keys[mid + 51] - 1, prng);
  keys_to_insert.insert(keys_to_insert.end(), left.begin(), left.end());
  keys_to_insert.insert(keys_to_insert.end(), right.begin(), right.end());

  // Create insert records.
  const std::string inserted_value = u8"08+bytes";
  std::vector<std::pair<uint64_t, Slice>> inserts;
  inserts.reserve(keys_to_insert.size());
  for (const auto& key : keys_to_insert) {
    inserts.emplace_back(key, inserted_value);
  }
  std::sort(inserts.begin(), inserts.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

  // Expected database contents (for testing).
  std::vector<std::pair<uint64_t, Slice>> all_records;
  all_records.reserve(dataset.size() + inserts.size());
  std::merge(dataset.begin(), dataset.end(), inserts.begin(), inserts.end(),
             std::back_inserter(all_records),
             [](const auto& left, const auto& right) {
               return left.first < right.first;
             });

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Make the inserts.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(1, all_records.size() + 1000000, &values);
    ASSERT_EQ(values.size(), all_records.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, all_records[i].first);
      ASSERT_EQ(all_records[i].second.compare(values[i].second), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Read the inserted values using a scan.
    std::vector<std::pair<uint64_t, std::string>> values;
    m.Scan(1, all_records.size() + 1000000, &values);
    ASSERT_EQ(values.size(), all_records.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(values[i].first, all_records[i].first);
      ASSERT_EQ(all_records[i].second.compare(values[i].second), 0);
    }
  }
}

}  // namespace
