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

namespace {

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

Manager::Options GetOptions(size_t goal, size_t delta, bool use_segments) {
  Manager::Options options;
  options.records_per_page_goal = goal;
  options.records_per_page_delta = delta;
  options.use_segments = use_segments;
  options.write_debug_info = false;
  options.use_memory_based_io = true;
  options.num_bg_threads = 0;
  return options;
}

TEST_F(ManagerTest, CreateReopenSegments) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);

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
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/false);

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
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);

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
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/false);

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
  Manager::Options options =
      GetOptions(/*goal=*/45, /*delta=*/5, /*use_segments=*/true);

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

void ValidateScanResults(
    const size_t start_idx, const size_t amount,
    const std::vector<std::pair<uint64_t, Slice>>& dataset,
    const std::vector<std::pair<uint64_t, std::string>>& scanned) {
  ASSERT_EQ(scanned.size(), amount);
  for (size_t i = 0; i < amount; ++i) {
    ASSERT_EQ(dataset[start_idx + i].first, scanned[i].first);
    ASSERT_EQ(dataset[start_idx + i].second.compare(scanned[i].second), 0);
  }
}

const std::vector<std::pair<size_t, size_t>> kScanRequests = ([]() {
  // Select "random" dataset indices to scan.
  const size_t num_starts = 50;
  const size_t max_length = 200;
  const size_t dataset_size = Datasets::kUniformKeys.size();

  std::vector<std::pair<size_t, size_t>> to_scan;
  to_scan.reserve(num_starts);
  std::uniform_int_distribution<size_t> start_dist(
      0, dataset_size - 1 - max_length),
      length_dist(1, max_length);
  std::mt19937 prng(42);
  for (size_t i = 0; i < num_starts; ++i) {
    to_scan.emplace_back(start_dist(prng), length_dist(prng));
  }
  return to_scan;
})();

TEST_F(ManagerTest, ScanSegments) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.Scan(dataset[start_idx].first, scan_amount, &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(ManagerTest, ScanSegmentsSequential) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kSequentialKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.Scan(dataset[start_idx].first, scan_amount, &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(ManagerTest, ScanPages) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/false);

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 1);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.Scan(dataset[start_idx].first, scan_amount, &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(ManagerTest, BatchedUpdateSegments) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Select 20 "random" dataset indices to update and read.
  std::vector<size_t> to_read;
  to_read.reserve(20);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 20; ++i) {
    to_read.push_back(dist(prng));
  }

  // Create updates.
  const std::string new_value = u8"08-bytes";
  std::vector<std::pair<uint64_t, Slice>> updates;
  updates.reserve(to_read.size());
  for (const auto& idx : to_read) {
    const std::pair<uint64_t, Slice>& rec = dataset[idx];
    updates.emplace_back(rec.first, new_value);
  }

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Read from newly loaded dataset.
    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }

    // Make the updates.
    ASSERT_TRUE(m.PutBatch(updates).ok());

    // Reading again should produce the new value.
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(Slice(out).compare(new_value), 0);
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
      ASSERT_EQ(Slice(out).compare(new_value), 0);
    }
  }
}

TEST_F(ManagerTest, BatchedUpdatePages) {
  Manager::Options options =
      GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/false);

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  // Select 20 "random" dataset indices to update and read.
  std::vector<size_t> to_read;
  to_read.reserve(20);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 20; ++i) {
    to_read.push_back(dist(prng));
  }

  // Create updates.
  const std::string new_value = u8"08-bytes";
  std::vector<std::pair<uint64_t, Slice>> updates;
  updates.reserve(to_read.size());
  for (const auto& idx : to_read) {
    const std::pair<uint64_t, Slice>& rec = dataset[idx];
    updates.emplace_back(rec.first, new_value);
  }

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Read from newly loaded dataset.
    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);
    }

    // Make the updates.
    ASSERT_TRUE(m.PutBatch(updates).ok());

    // Reading again should produce the new value.
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(Slice(out).compare(new_value), 0);
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
      ASSERT_EQ(Slice(out).compare(new_value), 0);
    }
  }
}

TEST_F(ManagerTest, InsertOverflowSegments) {
  Manager::Options options =
      GetOptions(/*goal=*/4, /*delta=*/1, /*use_segments=*/true);

  // 512 B string.
  std::string value;
  value.resize(512);

  std::vector<std::pair<uint64_t, Slice>> dataset = {
      {0, value}, {1, value}, {2, value}, {3, value},
      {4, value}, {5, value}, {6, value}};

  std::vector<std::pair<uint64_t, Slice>> inserts = {{7, value},  {8, value},
                                                     {9, value},  {10, value},
                                                     {11, value}, {12, value}};

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    // Make the inserts. These should go on the last page and should create an
    // overflow.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Make sure we can read the inserted records.
    std::string out;
    for (const auto& rec : inserts) {
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(value), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);

    std::string out;
    for (const auto& rec : inserts) {
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(value), 0);
    }

    // Scan and check combined.
    std::vector<std::pair<uint64_t, Slice>> combined;
    combined.reserve(dataset.size() + inserts.size());
    combined.insert(combined.end(), dataset.begin(), dataset.end());
    combined.insert(combined.end(), inserts.begin(), inserts.end());

    std::vector<std::pair<uint64_t, std::string>> scan_out;
    ASSERT_TRUE(m.Scan(0, 15, &scan_out).ok());
    ASSERT_EQ(scan_out.size(), combined.size());
    ValidateScanResults(0, combined.size(), combined, scan_out);
  }
}

TEST_F(ManagerTest, InsertOverflowPages) {
  Manager::Options options =
      GetOptions(/*goal=*/4, /*delta=*/1, /*use_segments=*/false);

  // 512 B string.
  std::string value;
  value.resize(512);

  std::vector<std::pair<uint64_t, Slice>> dataset = {
      {0, value}, {1, value}, {2, value}, {3, value},
      {4, value}, {5, value}, {6, value}};

  std::vector<std::pair<uint64_t, Slice>> inserts = {{7, value},  {8, value},
                                                     {9, value},  {10, value},
                                                     {11, value}, {12, value}};

  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    // Make the inserts. These should go on the last page and should create an
    // overflow.
    ASSERT_TRUE(m.PutBatch(inserts).ok());

    // Make sure we can read the inserted records.
    std::string out;
    for (const auto& rec : inserts) {
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(value), 0);
    }
  }

  // Read from reopened DB.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);

    std::string out;
    for (const auto& rec : inserts) {
      ASSERT_TRUE(m.Get(rec.first, &out).ok());
      ASSERT_EQ(rec.second.compare(value), 0);
    }

    // Scan and check combined.
    std::vector<std::pair<uint64_t, Slice>> combined;
    combined.reserve(dataset.size() + inserts.size());
    combined.insert(combined.end(), dataset.begin(), dataset.end());
    combined.insert(combined.end(), inserts.begin(), inserts.end());

    std::vector<std::pair<uint64_t, std::string>> scan_out;
    ASSERT_TRUE(m.Scan(0, 15, &scan_out).ok());
    ASSERT_EQ(scan_out.size(), combined.size());
    ValidateScanResults(0, combined.size(), combined, scan_out);
  }
}

}  // namespace
