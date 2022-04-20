#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "treeline/options.h"
#include "treeline/status.h"
#include "wal/manager.h"

namespace {

using namespace tl;
namespace fs = std::filesystem;

class WALManagerTest : public testing::Test {
 public:
  WALManagerTest()
      : kWALDir("/tmp/tl-wal-" + std::to_string(std::time(nullptr))) {}

  void SetUp() override {
    fs::remove_all(kWALDir);
    fs::create_directory(kWALDir);
  }

  void TearDown() override { fs::remove_all(kWALDir); }

  size_t NumFilesInDir() const {
    size_t count = 0;
    for (const auto& _ : fs::directory_iterator(kWALDir)) {
      ++count;
    }
    return count;
  }

  const fs::path kWALDir;
};

TEST_F(WALManagerTest, ReplayEmpty) {
  wal::Manager manager(kWALDir);
  Status s = manager.PrepareForReplay();
  ASSERT_TRUE(s.ok());

  size_t call_count = 0;
  s = manager.ReplayLog([&call_count](const Slice& key, const Slice& value,
                                      format::WriteType type) {
    ++call_count;
    return Status::OK();
  });
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(call_count, 0);
}

TEST_F(WALManagerTest, WriteThenReadOneVersion) {
  const std::vector<std::pair<std::string, std::string>> records = {
      {"hello", "world"}, {"key", "value"}, {"foo", "bar"}};
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForWrite();
    ASSERT_TRUE(s.ok());
    // Log file is lazily created.
    ASSERT_EQ(NumFilesInDir(), 0);

    for (const auto& record : records) {
      s = manager.LogWrite(WriteOptions(), record.first, record.second,
                           format::WriteType::kWrite);
      ASSERT_TRUE(s.ok());
    }
    ASSERT_EQ(NumFilesInDir(), 1);
  }
  ASSERT_EQ(NumFilesInDir(), 1);

  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());

    auto it = records.begin();
    const auto rec_end = records.end();
    s = manager.ReplayLog([&it, &rec_end](const Slice& key, const Slice& value,
                                          format::WriteType type) {
      EXPECT_TRUE(it != rec_end);
      EXPECT_TRUE(key.compare(it->first) == 0);
      EXPECT_TRUE(value.compare(it->second) == 0);
      EXPECT_EQ(type, format::WriteType::kWrite);
      ++it;
      return Status::OK();
    });
    ASSERT_TRUE(it == rec_end);
  }
  // Didn't delete the log - so it should still exist.
  ASSERT_EQ(NumFilesInDir(), 1);
}

TEST_F(WALManagerTest, CleanShutdown) {
  wal::Manager manager(kWALDir);
  Status s = manager.PrepareForWrite();
  ASSERT_TRUE(s.ok());
  s = manager.LogWrite(WriteOptions(), "hello", "world",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 1);

  uint64_t prev_version = manager.IncrementLogVersion();
  ASSERT_EQ(prev_version, 0);
  // The new log file is created lazily on the first write.
  ASSERT_EQ(NumFilesInDir(), 1);

  s = manager.LogWrite(WriteOptions(), "hello", "world",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 2);

  // On a clean shutdown, all volatile data is persisted to disk. We then
  // discard the write-ahead log(s).
  s = manager.DiscardAllForCleanShutdown();
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 0);
}

TEST_F(WALManagerTest, WriteReplayMultiple) {
  // Tests the scenario where we write to multiple logs, discard some logs
  // (because they are no longer needed), and then "crash". Make sure we can
  // replay the persisted logs.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForWrite();
    ASSERT_TRUE(s.ok());
    s = manager.LogWrite(WriteOptions(), "hello-0", "world-0",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    s = manager.LogWrite(WriteOptions(), "hello-0-1", "world-0-1",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 1);

    uint64_t prev_version = manager.IncrementLogVersion();
    ASSERT_EQ(prev_version, 0);
    // New log file created lazily.
    ASSERT_EQ(NumFilesInDir(), 1);

    s = manager.LogWrite(WriteOptions(), "hello-1", "world-1",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 2);

    prev_version = manager.IncrementLogVersion();
    ASSERT_EQ(prev_version, 1);
    ASSERT_EQ(NumFilesInDir(), 2);

    s = manager.LogWrite(WriteOptions(), "hello-2", "world-2",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 3);

    prev_version = manager.IncrementLogVersion();
    ASSERT_EQ(prev_version, 2);
    ASSERT_EQ(NumFilesInDir(), 3);

    // Discard the oldest log version
    s = manager.DiscardOldest(0);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 2);
  }
  // No logs should have been lost.
  ASSERT_EQ(NumFilesInDir(), 2);

  // Start replaying the logs.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());

    std::vector<std::pair<std::string, std::string>> records;
    s = manager.ReplayLog([&records](const Slice& key, const Slice& value,
                                     format::WriteType type) {
      EXPECT_EQ(type, format::WriteType::kWrite);
      records.emplace_back(std::string(key.data(), key.size()),
                           std::string(value.data(), value.size()));
      return Status::OK();
    });

    ASSERT_EQ(records.size(), 2);
    ASSERT_EQ(records[0].first, "hello-1");
    ASSERT_EQ(records[0].second, "world-1");
    ASSERT_EQ(records[1].first, "hello-2");
    ASSERT_EQ(records[1].second, "world-2");

    // Switch to writing mode (recovery has "finished")
    s = manager.PrepareForWrite();
    ASSERT_TRUE(s.ok());

    // By default we don't delete the replayed logs. The new log file is lazily
    // created, so there should be no change in the number of files.
    ASSERT_EQ(NumFilesInDir(), 2);
  }

  // Ensure that if the user requests old logs to be discarded, they are.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 2);

    s = manager.PrepareForWrite(/*discard_existing_logs=*/true);
    ASSERT_TRUE(s.ok());

    // No log files left.
    ASSERT_EQ(NumFilesInDir(), 0);

    // The new log's version was supposed to be 0.
    const uint64_t prev_version = manager.IncrementLogVersion();
    ASSERT_EQ(prev_version, 0);

    // But since no log writes were made, no log files should exist.
    ASSERT_EQ(NumFilesInDir(), 0);
  }
}

TEST_F(WALManagerTest, ReplayCleanShutdown) {
  // Replay after a clean shutdown should be a no-op.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForWrite();
    ASSERT_TRUE(s.ok());
    s = manager.LogWrite(WriteOptions(), "hello", "world",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());

    s = manager.DiscardAllForCleanShutdown();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 0);
  }
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 0);

    size_t call_count = 0;
    s = manager.ReplayLog([&call_count](const Slice& key, const Slice& value,
                                        format::WriteType type) {
      ++call_count;
      return Status::OK();
    });
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(call_count, 0);
  }
}

TEST_F(WALManagerTest, DiscardUpToInclusive) {
  wal::Manager manager(kWALDir);
  Status s = manager.PrepareForWrite();
  ASSERT_TRUE(s.ok());
  s = manager.LogWrite(WriteOptions(), "hello-0", "world-0",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 1);

  uint64_t prev_version = manager.IncrementLogVersion();
  ASSERT_EQ(prev_version, 0);
  // Log file is lazily created.
  ASSERT_EQ(NumFilesInDir(), 1);

  s = manager.LogWrite(WriteOptions(), "hello-1", "world-1",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 2);

  prev_version = manager.IncrementLogVersion();
  ASSERT_EQ(prev_version, 1);
  ASSERT_EQ(NumFilesInDir(), 2);

  s = manager.LogWrite(WriteOptions(), "hello-2", "world-2",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 3);

  prev_version = manager.IncrementLogVersion();
  ASSERT_EQ(prev_version, 2);
  ASSERT_EQ(NumFilesInDir(), 3);

  s = manager.LogWrite(WriteOptions(), "hello-3", "world-3",
                       format::WriteType::kWrite);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 4);

  // The current version is 3, but it is "active" and cannot be discarded.
  // If we request to discard up to 3 (inclusive), we should only delete logs 0,
  // 1, and 2.
  s = manager.DiscardUpToInclusive(3);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(NumFilesInDir(), 1);
  ASSERT_TRUE(fs::is_regular_file(kWALDir / "3"));
}

TEST_F(WALManagerTest, HandleUnrelatedFilePresence) {
  // Create a file in the WAL directory that is not related to the WAL.
  // The manager should just ignore this file.
  {
    std::ofstream unrelated(kWALDir / "123-not-log");
    unrelated << "Hello world!" << std::endl;
  }
  ASSERT_EQ(NumFilesInDir(), 1);

  // Write some data to the log.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForWrite(/*discard_existing_logs=*/true);
    ASSERT_TRUE(s.ok());
    // The unrelated file should not have been deleted.
    ASSERT_EQ(NumFilesInDir(), 1);

    s = manager.LogWrite(WriteOptions(), "hello-0", "world-0",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 2);
    ASSERT_EQ(manager.IncrementLogVersion(), 0);

    s = manager.LogWrite(WriteOptions(), "hello-1", "world-1",
                         format::WriteType::kWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 3);
  }

  // Make sure we can read the written records.
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(NumFilesInDir(), 3);

    std::vector<std::pair<std::string, std::string>> records;
    s = manager.ReplayLog([&records](const Slice& key, const Slice& value,
                                     format::WriteType type) {
      EXPECT_EQ(type, format::WriteType::kWrite);
      records.emplace_back(std::string(key.data(), key.size()),
                           std::string(value.data(), value.size()));
      return Status::OK();
    });

    ASSERT_EQ(records.size(), 2);
    ASSERT_EQ(records[0].first, "hello-0");
    ASSERT_EQ(records[0].second, "world-0");
    ASSERT_EQ(records[1].first, "hello-1");
    ASSERT_EQ(records[1].second, "world-1");

    s = manager.PrepareForWrite(/*discard_existing_logs=*/true);
    ASSERT_TRUE(s.ok());
    // Only the unrelated file should exist.
    ASSERT_EQ(NumFilesInDir(), 1);
  }
}

TEST_F(WALManagerTest, AbortReplayEarly) {
  const std::vector<std::pair<std::string, std::string>> records = {
      {"hello", "world"}, {"key", "value"}, {"foo", "bar"}};
  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForWrite();
    ASSERT_TRUE(s.ok());
    // Log file is lazily created.
    ASSERT_EQ(NumFilesInDir(), 0);

    for (const auto& record : records) {
      s = manager.LogWrite(WriteOptions(), record.first, record.second,
                           format::WriteType::kWrite);
      ASSERT_TRUE(s.ok());
    }
    ASSERT_EQ(NumFilesInDir(), 1);
  }

  {
    wal::Manager manager(kWALDir);
    Status s = manager.PrepareForReplay();
    ASSERT_TRUE(s.ok());

    // Ensure we abort early if the callback returns a non-OK status.
    size_t records_seen = 0;
    s = manager.ReplayLog([&records_seen](const Slice& key, const Slice& value,
                                          format::WriteType write_type) {
      ++records_seen;
      return Status::NotFound("Expected error.");
    });
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(records_seen, 1);
    ASSERT_TRUE(records_seen < records.size());

    // Replaying the log should now go through all the records.
    records_seen = 0;
    s = manager.ReplayLog([&records_seen](const Slice& key, const Slice& value,
                                          format::WriteType write_type) {
      ++records_seen;
      return Status::OK();
    });
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(records_seen, records.size());
  }
}

}  // namespace
