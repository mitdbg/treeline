#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "page_grouping/key.h"
#include "page_grouping/manager.h"
#include "page_grouping/segment_info.h"
#include "pg_datasets.h"
#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "util/key.h"

using namespace tl;
using namespace tl::pg;

namespace {

class PGManagerTest : public testing::Test {
 public:
  PGManagerTest()
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

TEST_F(PGManagerTest, CreateReopenSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);

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

TEST_F(PGManagerTest, CreateReopenPages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);

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

TEST_F(PGManagerTest, PointReadSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);

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

TEST_F(PGManagerTest, PointReadPages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);

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

TEST_F(PGManagerTest, PointReadSegmentsSequential) {
  auto options = GetOptions(/*goal=*/44, /*epsilon=*/5, /*use_segments=*/true);

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

TEST_F(PGManagerTest, ScanSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);

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

TEST_F(PGManagerTest, ScanSegmentsSequential) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);

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

TEST_F(PGManagerTest, ScanSegmentsPrefetch) {
  auto options = GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);
  options.num_bg_threads = 16;  // Must be non-zero for prefetching.

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.ScanWithExperimentalPrefetching(dataset[start_idx].first, scan_amount,
                                      &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(PGManagerTest, ScanSegmentsSequentialPrefetch) {
  auto options = GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/true);
  options.num_bg_threads = 16;  // Must be non-zero for prefetching.

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kSequentialKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 5);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.ScanWithExperimentalPrefetching(dataset[start_idx].first, scan_amount,
                                      &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(PGManagerTest, ScanPages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);

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

TEST_F(PGManagerTest, ScanPagesPrefetch) {
  auto options = GetOptions(/*goal=*/15, /*delta=*/5, /*use_segments=*/false);
  options.num_bg_threads = 16;  // Must be non-zero for prefetching.

  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kUniformKeys, u8"08 bytes");

  Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
  ASSERT_EQ(m.NumSegmentFiles(), 1);

  std::vector<std::pair<uint64_t, std::string>> scanned;
  for (const auto& [start_idx, scan_amount] : kScanRequests) {
    m.ScanWithExperimentalPrefetching(dataset[start_idx].first, scan_amount,
                                      &scanned);
    ValidateScanResults(start_idx, scan_amount, dataset, scanned);
  }
}

TEST_F(PGManagerTest, BatchedUpdateSegments) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/true);

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
  std::sort(updates.begin(), updates.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

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

TEST_F(PGManagerTest, BatchedUpdatePages) {
  auto options = GetOptions(/*goal=*/15, /*epsilon=*/5, /*use_segments=*/false);

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
  std::sort(updates.begin(), updates.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });

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

TEST_F(PGManagerTest, InsertOverflowSegments) {
  auto options = GetOptions(/*goal=*/4, /*epsilon=*/1, /*use_segments=*/true);

  // 512 B string.
  std::string value;
  value.resize(512);

  std::vector<std::pair<uint64_t, Slice>> dataset = {
      {1, value}, {2, value}, {3, value}, {4, value},
      {5, value}, {6, value}, {7, value}};

  std::vector<std::pair<uint64_t, Slice>> inserts = {{8, value},  {9, value},
                                                     {10, value}, {11, value},
                                                     {12, value}, {13, value}};

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
    ASSERT_TRUE(m.Scan(1, 15, &scan_out).ok());
    ASSERT_EQ(scan_out.size(), combined.size());
    ValidateScanResults(0, combined.size(), combined, scan_out);
  }
}

TEST_F(PGManagerTest, InsertOverflowPages) {
  auto options = GetOptions(/*goal=*/4, /*epsilon=*/1, /*use_segments=*/false);

  // 512 B string.
  std::string value;
  value.resize(512);

  std::vector<std::pair<uint64_t, Slice>> dataset = {
      {1, value}, {2, value}, {3, value}, {4, value},
      {5, value}, {6, value}, {7, value}};

  std::vector<std::pair<uint64_t, Slice>> inserts = {{8, value},  {9, value},
                                                     {10, value}, {11, value},
                                                     {12, value}, {13, value}};

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
    ASSERT_TRUE(m.Scan(1, 15, &scan_out).ok());
    ASSERT_EQ(scan_out.size(), combined.size());
    ValidateScanResults(0, combined.size(), combined, scan_out);
  }
}

TEST_F(PGManagerTest, PageBoundsConsistency) {
  auto options = GetOptions(/*goal=*/44, /*epsilon=*/5, /*use_segments=*/true);

  // Use the sequential dataset instead of the uniform one.
  std::vector<std::pair<uint64_t, Slice>> dataset =
      BuildRecords(Datasets::kSequentialKeys, u8"08 bytes");

  // Select 50 "random" dataset indices.
  std::vector<size_t> to_read;
  to_read.reserve(50);
  std::uniform_int_distribution<size_t> dist(0, dataset.size() - 1);
  std::mt19937 prng(42);
  for (size_t i = 0; i < 50; ++i) {
    to_read.push_back(dist(prng));
  }

  const auto run_checks = [&dataset, &to_read](Manager& m) {
    std::string out;
    for (const auto& idx : to_read) {
      const std::pair<uint64_t, Slice>& rec = dataset[idx];
      const auto& [status, pages] = m.GetWithPages(rec.first, &out);

      // Successful read.
      ASSERT_TRUE(status.ok());
      ASSERT_EQ(rec.second.compare(Slice(out)), 0);

      // No overflows.
      ASSERT_EQ(pages.size(), 1);

      // Check that the page boundaries stored on disk are consistent with the
      // ones computed from the model.
      const Key stored_lower =
          key_utils::ExtractHead64(pages[0].GetLowerBoundary());
      const Key stored_upper =
          key_utils::ExtractHead64(pages[0].GetUpperBoundary());

      const auto [computed_lower, computed_upper] =
          m.GetPageBoundsFor(rec.first);
      ASSERT_EQ(computed_lower, stored_lower);
      // The on-disk page stores an inclusive upper bound. All other upper
      // bounds in the page grouping code are exclusive.
      ASSERT_EQ(computed_upper, stored_upper + 1);
    }
  };

  // Check newly loaded DB that uses segments.
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);
    run_checks(m);
  }
  // Check reopened DB that uses segments.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 5);
    run_checks(m);
  }

  // Check newly loaded DB that uses single-page segments only.
  std::filesystem::remove_all(kDBDir);
  std::filesystem::create_directory(kDBDir);
  options.use_segments = false;
  {
    Manager m = Manager::LoadIntoNew(kDBDir, dataset, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);
    run_checks(m);
  }
  // Check reopened DB that uses single-page segments only.
  {
    Manager m = Manager::Reopen(kDBDir, options);
    ASSERT_EQ(m.NumSegmentFiles(), 1);
    run_checks(m);
  }
}

}  // namespace
