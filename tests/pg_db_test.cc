#include "llsm/pg_db.h"

#include <algorithm>
#include <filesystem>
#include <numeric>
#include <vector>

#include "gtest/gtest.h"
#include "llsm/pg_options.h"

namespace {

using namespace llsm;
using namespace llsm::pg;

class PGDBTest : public testing::Test {
 public:
  PGDBTest()
      : kDBDir("/tmp/pg-llsm-test-" + std::to_string(std::time(nullptr))) {}
  void SetUp() override {
    std::filesystem::remove_all(kDBDir);
    std::filesystem::create_directory(kDBDir);
  }
  void TearDown() override { std::filesystem::remove_all(kDBDir); }

  const std::filesystem::path kDBDir;
};

PageGroupedDBOptions GetCommonTestOptions() {
  PageGroupedDBOptions options;
  options.use_memory_based_io = true;
  options.num_bg_threads = 2;
  return options;
}

std::vector<Record> GetRangeDataset(const Key step, size_t num_records,
                                    const std::string& value) {
  std::vector<size_t> indices;
  indices.resize(num_records);
  std::iota(indices.begin(), indices.end(), 1ULL);

  std::vector<Record> records;
  records.resize(num_records);
  std::transform(indices.begin(), indices.end(), records.begin(),
                 [&step, &value](const size_t idx) {
                   return std::make_pair(idx * step, Slice(value));
                 });
  return records;
}

// The tests in this file are simple sanity checks.

TEST_F(PGDBTest, LoadReadWriteRead) {
  PageGroupedDB* db = nullptr;
  auto options = GetCommonTestOptions();
  options.records_per_page_goal = 45;
  options.records_per_page_delta = 5;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Load.
  const std::string value = "Test 1";
  const auto dataset = GetRangeDataset(10, 1000, value);
  ASSERT_TRUE(db->BulkLoad(dataset).ok());

  // Read.
  std::string out;
  ASSERT_TRUE(db->Get(10, &out).ok());
  ASSERT_EQ(out, value);
  ASSERT_TRUE(db->Get(102, &out).IsNotFound());

  // Write.
  const std::string new_value = "Test 2";
  ASSERT_TRUE(db->Put(102, new_value).ok());
  ASSERT_TRUE(db->Put(20, new_value).ok());

  // Read.
  ASSERT_TRUE(db->Get(10, &out).ok());
  ASSERT_EQ(out, value);
  ASSERT_TRUE(db->Get(102, &out).ok());
  ASSERT_EQ(out, new_value);
  ASSERT_TRUE(db->Get(20, &out).ok());
  ASSERT_EQ(out, new_value);
  ASSERT_TRUE(db->Get(33, &out).IsNotFound());

  // Close the DB.
  delete db;
  db = nullptr;
}

TEST_F(PGDBTest, LoadWriteScanReopenScan) {
  PageGroupedDB* db = nullptr;
  auto options = GetCommonTestOptions();
  options.records_per_page_goal = 45;
  options.records_per_page_delta = 5;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Load.
  const std::string value = "Test 1";
  const auto dataset = GetRangeDataset(10, 1000, value);
  ASSERT_TRUE(db->BulkLoad(dataset).ok());

  // Write.
  const std::string new_value = "Test 2";
  ASSERT_TRUE(db->Put(102, new_value).ok());
  ASSERT_TRUE(db->Put(20, new_value).ok());
  ASSERT_TRUE(db->Put(1001, new_value).ok());

  // Scan.
  std::vector<Record> expected(dataset);
  expected[1].second = new_value;
  expected.emplace_back(102, new_value);
  expected.emplace_back(1001, new_value);
  std::sort(expected.begin(), expected.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

  std::vector<std::pair<Key, std::string>> scan_out;
  ASSERT_TRUE(db->GetRange(1, 2000, &scan_out).ok());
  ASSERT_EQ(scan_out.size(), expected.size());
  for (size_t i = 0; i < scan_out.size(); ++i) {
    ASSERT_EQ(scan_out[i].first, expected[i].first);
    ASSERT_EQ(expected[i].second.compare(scan_out[i].second), 0);
  }

  // Reopen.
  delete db;
  db = nullptr;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Run the same scan again.
  scan_out.clear();
  ASSERT_TRUE(db->GetRange(1, 2000, &scan_out).ok());
  ASSERT_EQ(scan_out.size(), expected.size());
  for (size_t i = 0; i < scan_out.size(); ++i) {
    ASSERT_EQ(scan_out[i].first, expected[i].first);
    ASSERT_EQ(expected[i].second.compare(scan_out[i].second), 0);
  }

  // Close the DB.
  delete db;
  db = nullptr;
}

TEST_F(PGDBTest, ScanAmount) {
  PageGroupedDB* db = nullptr;
  auto options = GetCommonTestOptions();
  options.records_per_page_goal = 45;
  options.records_per_page_delta = 5;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Load.
  const std::string value = "Test 1";
  const auto dataset = GetRangeDataset(10, 1000, value);
  ASSERT_TRUE(db->BulkLoad(dataset).ok());

  // Write (insert).
  const std::string new_value = "Test 2";
  ASSERT_TRUE(db->Put(101, new_value).ok());
  ASSERT_TRUE(db->Put(102, new_value).ok());
  ASSERT_TRUE(db->Put(103, new_value).ok());

  const std::vector<Record> expected = {{101, Slice(new_value)},
                                        {102, Slice(new_value)},
                                        {103, Slice(new_value)}};

  // Scan.
  std::vector<std::pair<Key, std::string>> scan_out;
  ASSERT_TRUE(db->GetRange(101, 3, &scan_out).ok());
  ASSERT_EQ(scan_out.size(), expected.size());
  for (size_t i = 0; i < scan_out.size(); ++i) {
    ASSERT_EQ(scan_out[i].first, expected[i].first);
    ASSERT_EQ(expected[i].second.compare(scan_out[i].second), 0);
  }

  // Close the DB.
  delete db;
  db = nullptr;
}

TEST_F(PGDBTest, LoadParallelFlushReopenScan) {
  PageGroupedDB* db = nullptr;
  auto options = GetCommonTestOptions();
  options.records_per_page_goal = 45;
  options.records_per_page_delta = 5;
  options.parallelize_final_flush = true;
  options.num_bg_threads = 3;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Load.
  const std::string value = "Test 1";
  const auto dataset = GetRangeDataset(10, 1000, value);
  ASSERT_TRUE(db->BulkLoad(dataset).ok());

  // Write (update).
  const std::string new_value = "Test 2";
  ASSERT_TRUE(db->Put(100, new_value).ok());
  ASSERT_TRUE(db->Put(20, new_value).ok());
  ASSERT_TRUE(db->Put(510, new_value).ok());

  // Reopen.
  delete db;
  db = nullptr;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Scan.
  std::vector<Record> expected(dataset);
  expected[1].second = new_value;
  expected[9].second = new_value;
  expected[50].second = new_value;
  std::sort(expected.begin(), expected.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

  std::vector<std::pair<Key, std::string>> scan_out;
  ASSERT_TRUE(db->GetRange(1, 2000, &scan_out).ok());
  ASSERT_EQ(scan_out.size(), expected.size());
  for (size_t i = 0; i < scan_out.size(); ++i) {
    ASSERT_EQ(scan_out[i].first, expected[i].first);
    ASSERT_EQ(expected[i].second.compare(scan_out[i].second), 0);
  }

  // Close the DB.
  delete db;
  db = nullptr;
}

TEST_F(PGDBTest, InsertSmaller) {
  PageGroupedDB* db = nullptr;
  auto options = GetCommonTestOptions();
  options.records_per_page_goal = 2;
  options.records_per_page_delta = 1;
  options.parallelize_final_flush = true;
  options.num_bg_threads = 3;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  const char kLargeValue[1024] = {};
  const Slice kLargeValueSlice(kLargeValue, 1024);

  // Load.
  const std::string value = "Test 1";
  const std::vector<Record> dataset = {
      {10, kLargeValueSlice}, {100, kLargeValueSlice}, {200, kLargeValueSlice}};
  ASSERT_TRUE(db->BulkLoad(dataset).ok());

  // Insert a few keys that are smaller than the previous smallest key.
  ASSERT_TRUE(db->Put(9, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(8, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(7, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(6, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(5, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(4, kLargeValueSlice).ok());
  ASSERT_TRUE(db->Put(3, kLargeValueSlice).ok());

  // Reopen.
  delete db;
  db = nullptr;
  ASSERT_TRUE(PageGroupedDB::Open(options, kDBDir, &db).ok());
  ASSERT_NE(db, nullptr);

  // Scan the whole DB.
  std::vector<Record> expected = {
      {3, kLargeValueSlice},  {4, kLargeValueSlice},  {5, kLargeValueSlice},
      {6, kLargeValueSlice},  {7, kLargeValueSlice},  {8, kLargeValueSlice},
      {9, kLargeValueSlice},  {10, kLargeValueSlice}, {100, kLargeValueSlice},
      {200, kLargeValueSlice}};
  std::vector<std::pair<Key, std::string>> scan_out;
  ASSERT_TRUE(db->GetRange(1, 2000, &scan_out).ok());
  ASSERT_EQ(scan_out.size(), expected.size());
  for (size_t i = 0; i < scan_out.size(); ++i) {
    ASSERT_EQ(scan_out[i].first, expected[i].first);
    ASSERT_EQ(expected[i].second.compare(scan_out[i].second), 0);
  }

  // Close the DB.
  delete db;
  db = nullptr;
}

}  // namespace
