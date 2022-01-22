#include "../manager.h"

#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "../key.h"
#include "../segment_info.h"
#include "datasets.h"
#include "gtest/gtest.h"
#include "llsm/slice.h"

using namespace llsm;
using namespace llsm::pg;

class ManagerTest : public testing::Test {
 public:
  ManagerTest() : kDBDir("/tmp/llsm-pg-test") {}
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

TEST_F(ManagerTest, CreateReopenSegments) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");
  std::vector<std::pair<Key, SegmentInfo>> index_entries;
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);
    for (auto it = m.IndexBeginIterator(); it != m.IndexEndIterator(); ++it) {
      index_entries.push_back(*it);
    }
  }
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);
    std::vector<std::pair<Key, SegmentInfo>> deserialized_entries;
    deserialized_entries.reserve(index_entries.size());
    for (auto it = m.IndexBeginIterator(); it != m.IndexEndIterator(); ++it) {
      deserialized_entries.push_back(*it);
    }

    // Make equality assertions bit by bit to make debugging simpler.
    ASSERT_EQ(deserialized_entries.size(), index_entries.size());
    for (size_t i = 0; i < deserialized_entries.size(); ++i) {
      const std::pair<Key, SegmentInfo>& orig = index_entries[i];
      const std::pair<Key, SegmentInfo>& deser = deserialized_entries[i];
      ASSERT_EQ(deser.first, orig.first);
      ASSERT_EQ(deser.second.id(), orig.second.id());
      ASSERT_EQ(deser.second.model(), orig.second.model());
    }
  }
}

TEST_F(ManagerTest, CreateReopenPages) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_direct_io = false;
  options.write_debug_info = false;
  options.use_segments = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");
  std::vector<std::pair<Key, SegmentInfo>> index_entries;
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);
    for (auto it = m.IndexBeginIterator(); it != m.IndexEndIterator(); ++it) {
      index_entries.push_back(*it);
    }
  }
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);
    std::vector<std::pair<Key, SegmentInfo>> deserialized_entries;
    deserialized_entries.reserve(index_entries.size());
    for (auto it = m.IndexBeginIterator(); it != m.IndexEndIterator(); ++it) {
      deserialized_entries.push_back(*it);
    }

    // Make equality assertions bit by bit to make debugging simpler.
    ASSERT_EQ(deserialized_entries.size(), index_entries.size());
    for (size_t i = 0; i < deserialized_entries.size(); ++i) {
      const std::pair<Key, SegmentInfo>& orig = index_entries[i];
      const std::pair<Key, SegmentInfo>& deser = deserialized_entries[i];
      ASSERT_EQ(deser.first, orig.first);
      ASSERT_EQ(deser.second.id(), orig.second.id());
      ASSERT_EQ(deser.second.model(), orig.second.model());
    }
  }
}

TEST_F(ManagerTest, PointReadSegments) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Select 20 "random" dataset indices to read.
  std::vector<size_t> to_read;
  to_read.reserve(20);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 20; ++i) {
    to_read.push_back(dist(prng));
  }

  // Read from newly loaded dataset.
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }
}

TEST_F(ManagerTest, PointReadPages) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = false;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Select 20 "random" dataset indices to read.
  std::vector<size_t> to_read;
  to_read.reserve(20);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 20; ++i) {
    to_read.push_back(dist(prng));
  }

  // Read from newly loaded dataset.
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }
}

TEST_F(ManagerTest, PointReadSegmentsSequential) {
  Manager::Options options;
  options.records_per_page_goal = 45;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;

  // Use the sequential dataset instead of the uniform one.
  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kSequentialKeys, u8"08 bytes");

  // Select 20 "random" dataset indices to read.
  std::vector<size_t> to_read;
  to_read.reserve(20);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 20; ++i) {
    to_read.push_back(dist(prng));
  }

  // Read from newly loaded dataset.
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }
  }
}

TEST_F(ManagerTest, ScanSegments) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  const size_t start_idx = 10;
  const size_t scan_amount = 200;
  std::vector<std::pair<uint64_t, std::string>> scanned;
  m.Scan(dataset[start_idx].first, scan_amount, &scanned);
  ASSERT_EQ(scanned.size(), scan_amount);
  for (size_t i = 0; i < scan_amount; ++i) {
    ASSERT_EQ(dataset[start_idx + i].first, scanned[i].first);
    ASSERT_EQ(dataset[start_idx + i].second.compare(scanned[i].second), 0);
  }
}

TEST_F(ManagerTest, ScanSegmentsSequential) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kSequentialKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  const size_t start_idx = 15;
  const size_t scan_amount = 200;
  std::vector<std::pair<uint64_t, std::string>> scanned;
  m.Scan(dataset[start_idx].first, scan_amount, &scanned);
  ASSERT_EQ(scanned.size(), scan_amount);
  for (size_t i = 0; i < scan_amount; ++i) {
    ASSERT_EQ(dataset[start_idx + i].first, scanned[i].first);
    ASSERT_EQ(dataset[start_idx + i].second.compare(scanned[i].second), 0);
  }
}

TEST_F(ManagerTest, ScanPages) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = false;
  options.write_debug_info = false;
  options.use_direct_io = false;

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 1);

  const size_t start_idx = 10;
  const size_t scan_amount = 200;
  std::vector<std::pair<uint64_t, std::string>> scanned;
  m.Scan(dataset[start_idx].first, scan_amount, &scanned);
  ASSERT_EQ(scanned.size(), scan_amount);
  for (size_t i = 0; i < scan_amount; ++i) {
    ASSERT_EQ(dataset[start_idx + i].first, scanned[i].first);
    ASSERT_EQ(dataset[start_idx + i].second.compare(scanned[i].second), 0);
  }
}
