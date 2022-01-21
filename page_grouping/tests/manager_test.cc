#include "../manager.h"

#include <filesystem>
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

