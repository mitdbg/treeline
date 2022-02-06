#include <filesystem>
#include <numeric>
#include <utility>
#include <vector>

#include "../key.h"
#include "../manager.h"
#include "../segment_info.h"
#include "datasets.h"
#include "gtest/gtest.h"
#include "llsm/slice.h"

namespace {

using namespace llsm;
using namespace llsm::pg;

class ManagerRewriteTest : public testing::Test {
 public:
  ManagerRewriteTest() : kDBDir("/tmp/llsm-pg-test") {}
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

TEST_F(ManagerRewriteTest, AppendSegments) {
  Manager::Options options;
  options.records_per_page_goal = 15;
  options.records_per_page_delta = 5;
  options.use_segments = true;
  options.write_debug_info = false;
  options.use_direct_io = false;
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

}  // namespace
