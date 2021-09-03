#include "llsm/db.h"

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <random>
#include <vector>

#include "db/page.h"
#include "gtest/gtest.h"
#include "util/key.h"

namespace {

bool EqualTimespec(const timespec& lhs, const timespec& rhs) {
  return (lhs.tv_sec == rhs.tv_sec) && (lhs.tv_nsec == rhs.tv_nsec);
}

class DBTest : public testing::Test {
 public:
  DBTest() : kDBDir("/tmp/llsm-test") {}
  void SetUp() override {
    std::filesystem::remove_all(kDBDir);
    std::filesystem::create_directory(kDBDir);
  }
  void TearDown() override { std::filesystem::remove_all(kDBDir); }

  const std::filesystem::path kDBDir;
};

TEST_F(DBTest, Create) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  // The test environment may not have many cores.
  options.pin_threads = false;
  options.key_hints.num_keys = 10;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  delete db;
}

TEST_F(DBTest, CreateIfMissingDisabled) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.create_if_missing = false;
  options.pin_threads = false;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.IsInvalidArgument());
  ASSERT_EQ(db, nullptr);
}

TEST_F(DBTest, ErrorIfExistsEnabled) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.error_if_exists = true;
  options.pin_threads = false;
  options.key_hints.num_keys = 10;

  // Create the database and then close it.
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  delete db;
  db = nullptr;

  // Attempt to open it again (but with `error_if_exists` set to true).
  status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.IsInvalidArgument());
  ASSERT_EQ(db, nullptr);
}

TEST_F(DBTest, WriteFlushRead) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.num_keys = 10;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());

  const uint64_t key_as_int = __builtin_bswap64(1ULL);
  const std::string value = "Hello world!";
  llsm::Slice key(reinterpret_cast<const char*>(&key_as_int),
                  sizeof(key_as_int));
  status = db->Put(llsm::WriteOptions(), key, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  std::string value_out;
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Should be a page read (but will be cached in the buffer pool).
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
}

TEST_F(DBTest, WriteFlushReadNoHint) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.num_keys = 0;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());

  const uint64_t key_as_int = __builtin_bswap64(1ULL);
  const std::string value = "Hello world!";
  llsm::Slice key(reinterpret_cast<const char*>(&key_as_int),
                  sizeof(key_as_int));
  status = db->Put(llsm::WriteOptions(), key, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  std::string value_out;
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Should be a page read (but will be cached in the buffer pool).
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
}

TEST_F(DBTest, WriteThenDelete) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.num_keys = 10;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());

  const std::string value = "Hello world!";
  std::string value_out;

  //////////////////////////////////
  // 1. Everything in the memtable.
  const uint64_t key_as_int1 = __builtin_bswap64(1ULL);
  llsm::Slice key1(reinterpret_cast<const char*>(&key_as_int1),
                   sizeof(key_as_int1));
  // Write
  status = db->Put(llsm::WriteOptions(), key1, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  status = db->Get(llsm::ReadOptions(), key1, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  // Delete
  status = db->Delete(llsm::WriteOptions(), key1);
  ASSERT_TRUE(status.ok());

  // Should not find it.
  status = db->Get(llsm::ReadOptions(), key1, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  //////////////////////////////////
  // 2. Just write is flushed
  const uint64_t key_as_int2 = __builtin_bswap64(2ULL);
  llsm::Slice key2(reinterpret_cast<const char*>(&key_as_int2),
                   sizeof(key_as_int2));
  // Write
  status = db->Put(llsm::WriteOptions(), key2, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  status = db->Get(llsm::ReadOptions(), key2, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  // Flush
  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Delete
  status = db->Delete(llsm::WriteOptions(), key2);
  ASSERT_TRUE(status.ok());

  // Should not find it.
  status = db->Get(llsm::ReadOptions(), key2, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  //////////////////////////////////
  // 3. Both are flushed individually

  const uint64_t key_as_int3 = __builtin_bswap64(3ULL);
  llsm::Slice key3(reinterpret_cast<const char*>(&key_as_int3),
                   sizeof(key_as_int3));
  // Write
  status = db->Put(llsm::WriteOptions(), key3, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  status = db->Get(llsm::ReadOptions(), key3, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  // Flush
  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Delete
  status = db->Delete(llsm::WriteOptions(), key3);
  ASSERT_TRUE(status.ok());

  // Flush
  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Should not find it.
  status = db->Get(llsm::ReadOptions(), key3, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  //////////////////////////////////
  // 4. Both are flushed together

  const uint64_t key_as_int4 = __builtin_bswap64(4ULL);
  llsm::Slice key4(reinterpret_cast<const char*>(&key_as_int4),
                   sizeof(key_as_int4));
  // Write
  status = db->Put(llsm::WriteOptions(), key4, value);
  ASSERT_TRUE(status.ok());

  // Should be a memtable read.
  status = db->Get(llsm::ReadOptions(), key4, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  // Delete
  status = db->Delete(llsm::WriteOptions(), key4);
  ASSERT_TRUE(status.ok());

  // Flush
  status = db->FlushMemTable(/*disable_deferred_io = */ true);
  ASSERT_TRUE(status.ok());

  // Should not find it.
  status = db->Get(llsm::ReadOptions(), key4, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  delete db;
}

TEST_F(DBTest, DeferByEntries) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = 512;
  options.key_hints.key_size = 8;
  // Enough to be spread out over two pages.
  options.key_hints.num_keys = 2 * options.key_hints.records_per_page();
  options.deferred_io_batch_size = 40;
  options.deferred_io_max_deferrals = 4;
  options.buffer_pool_size = llsm::Page::kSize;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());

  const std::string value = "Hello world!";
  std::string value_out;

  // Write
  const uint64_t key_as_int1 = __builtin_bswap64(1ULL);
  llsm::Slice key1(reinterpret_cast<const char*>(&key_as_int1),
                   sizeof(key_as_int1));
  status = db->Put(llsm::WriteOptions(), key1, value);
  ASSERT_TRUE(status.ok());

  // Get timestamp
  auto filename = kDBDir / "segment-0";
  timespec mod_time;
  struct stat result;
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    mod_time = result.st_mtim;
  }

  // Flush - shouldn't flush anything
  status = db->FlushMemTable(/*disable_deferred_io = */ false);
  ASSERT_TRUE(status.ok());

  // Make sure page is evicted by looking up sth else.
  const uint64_t key_as_int9 = __builtin_bswap64(9ULL);
  llsm::Slice key9(reinterpret_cast<const char*>(&key_as_int9),
                   sizeof(key_as_int9));
  status = db->Get(llsm::ReadOptions(), key9, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  // Check that the flush never happened.
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    ASSERT_TRUE(EqualTimespec(result.st_mtim, mod_time));
  }

  // Write another to segment 0
  const uint64_t key_as_int0 = __builtin_bswap64(0ULL);
  llsm::Slice key0(reinterpret_cast<const char*>(&key_as_int0),
                   sizeof(key_as_int0));
  status = db->Put(llsm::WriteOptions(), key0, value);
  ASSERT_TRUE(status.ok());

  // Flush - should work now
  status = db->FlushMemTable(/*disable_deferred_io = */ false);
  ASSERT_TRUE(status.ok());

  // Make sure page is evicted by looking up sth else.
  status = db->Get(llsm::ReadOptions(), key9, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  // Check that the flush happened.
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    ASSERT_FALSE(EqualTimespec(result.st_mtim, mod_time));
  }

  // Can still read them
  status = db->Get(llsm::ReadOptions(), key1, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);
  status = db->Get(llsm::ReadOptions(), key0, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
}

TEST_F(DBTest, DeferByAttempts) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = 512;
  options.key_hints.key_size = 8;
  // Enough to be spread out over two pages.
  options.key_hints.num_keys = 2 * options.key_hints.records_per_page();
  options.deferred_io_batch_size = 2 * options.key_hints.record_size;
  options.deferred_io_max_deferrals = 1;
  options.buffer_pool_size = llsm::Page::kSize;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());

  const std::string value = "Hello world!";
  std::string value_out;

  // Write
  const uint64_t key_as_int1 = __builtin_bswap64(1ULL);
  llsm::Slice key1(reinterpret_cast<const char*>(&key_as_int1),
                   sizeof(key_as_int1));
  status = db->Put(llsm::WriteOptions(), key1, value);
  ASSERT_TRUE(status.ok());

  // Get timestamp
  auto filename = kDBDir / "segment-0";
  timespec mod_time;
  struct stat result;
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    mod_time = result.st_mtim;
  }

  // Flush - shouldn't flush anything
  status = db->FlushMemTable(/*disable_deferred_io = */ false);
  ASSERT_TRUE(status.ok());

  // Make sure page is evicted by looking up sth else.
  const uint64_t key_as_int9 = __builtin_bswap64(9ULL);
  llsm::Slice key9(reinterpret_cast<const char*>(&key_as_int9),
                   sizeof(key_as_int9));
  status = db->Get(llsm::ReadOptions(), key9, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  // Check that the flush never happened.
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    ASSERT_TRUE(EqualTimespec(result.st_mtim, mod_time));
  }

  // Flush - should work now
  status = db->FlushMemTable(/*disable_deferred_io = */ false);
  ASSERT_TRUE(status.ok());

  // Make sure page is evicted by looking up sth else.
  status = db->Get(llsm::ReadOptions(), key9, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  // Check that the flush happened.
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    ASSERT_FALSE(EqualTimespec(result.st_mtim, mod_time));
  }

  // Can still read
  status = db->Get(llsm::ReadOptions(), key1, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
}

TEST_F(DBTest, WriteReopenRead) {
  const std::string value = "Hello world!";

  // Will write 10 records with keys 0 - 9 and value `value`.
  llsm::Options options;
  options.pin_threads = false;
  options.key_hints.num_keys = 10;
  options.key_hints.record_size = sizeof(uint64_t) + value.size();
  options.key_hints.key_size = sizeof(uint64_t);
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  llsm::DB* db = nullptr;
  auto status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int),
                    sizeof(key_as_int));
    status = db->Put(llsm::WriteOptions(), key, value);
    ASSERT_TRUE(status.ok());
  }

  // Should be able to read all the data (in memory).
  std::string value_out;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int),
                    sizeof(key_as_int));
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, value_out);
  }

  // Close the database.
  delete db;
  db = nullptr;

  // Make sure an error occurs if the database does not exist when we try to
  // reopen it.
  options.create_if_missing = false;
  status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Should be able to read all the data back out (should be from disk).
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int),
                    sizeof(key_as_int));
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, value_out);
  }

  delete db;
}

TEST_F(DBTest, WriteReopenReadReverse) {
  // Write more than 256 pages of data per segment, close the database, reopen
  // it, read a record on the first page in each segment, and then read all the
  // records in reverse order.

  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 1016;

  // A dummy value used for all records (8 byte key; record is 1 KiB in total).
  const std::string value(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1024;
  options.key_hints.num_keys = 1024 * options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write all the data.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value);
    ASSERT_TRUE(status.ok());
  }

  // Close and then reopen the DB.
  delete db;
  db = nullptr;
  status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // First, read data in the first page in each segment.
  std::string value_out;
  const std::vector<uint64_t> relevant_keys = {__builtin_bswap64(0ULL),
                                               __builtin_bswap64(16384ULL)};
  for (const auto& key_as_int : relevant_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, value_out);
  }

  // Now read all the records, but in reverse order.
  for (auto it = lexicographic_keys.rbegin(); it != lexicographic_keys.rend();
       ++it) {
    llsm::Slice key(reinterpret_cast<const char*>(&(*it)), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, value_out);
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, RangeScan) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 1016;

  // Dummy values used for the records (8 byte key; record is 1 KiB in total).
  const std::string value_old(kValueSize, 0xFF);
  const std::string value_new(kValueSize, 0x00);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1024;
  options.key_hints.num_keys = 128 * options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Scan in memory.
  const size_t start_index = 10;
  const size_t num_records = options.key_hints.num_keys - 10;
  std::vector<llsm::Record> results;
  status = db->GetRange(
      llsm::ReadOptions(),
      llsm::Slice(
          reinterpret_cast<const char*>(&lexicographic_keys[start_index]), 8),
      num_records, &results);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(results.size(), num_records);

  for (size_t i = 0; i < num_records; ++i) {
    const uint64_t key =
        *reinterpret_cast<const uint64_t*>(results[i].key().data());
    ASSERT_EQ(key, lexicographic_keys[start_index + i]);
    ASSERT_EQ(results[i].value(), value_old);
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Scan from the pages.
  results.clear();
  status = db->GetRange(
      llsm::ReadOptions(),
      llsm::Slice(
          reinterpret_cast<const char*>(&lexicographic_keys[start_index]), 8),
      num_records, &results);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(results.size(), num_records);

  for (size_t i = 0; i < num_records; ++i) {
    const uint64_t key =
        *reinterpret_cast<const uint64_t*>(results[i].key().data());
    ASSERT_EQ(key, lexicographic_keys[start_index + i]);
    ASSERT_EQ(results[i].value(), value_old);
  }

  // Overwrite half of the existing records (but the writes will be in memory).
  for (size_t i = 0; i < lexicographic_keys.size(); ++i) {
    if (i % 2 != 0) continue;
    llsm::Slice key(reinterpret_cast<const char*>(&lexicographic_keys[i]),
                    kKeySize);
    status = db->Put(woptions, key, value_new);
    ASSERT_TRUE(status.ok());
  }

  // Scan again (some records should be in the memtable, some will be in the
  // pages).
  results.clear();
  status = db->GetRange(
      llsm::ReadOptions(),
      llsm::Slice(
          reinterpret_cast<const char*>(&lexicographic_keys[start_index]), 8),
      num_records, &results);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(results.size(), num_records);

  for (size_t i = 0; i < num_records; ++i) {
    const uint64_t key =
        *reinterpret_cast<const uint64_t*>(results[i].key().data());
    const size_t key_index = i + start_index;
    ASSERT_EQ(key, lexicographic_keys[key_index]);
    if (key_index % 2 == 0) {
      ASSERT_EQ(results[i].value(), value_new);
    } else {
      ASSERT_EQ(results[i].value(), value_old);
    }
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, OverflowByRecordNumber) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;

  // Dummy value used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // All records fit into 1 page
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Generate data for enough additional writes to definitely overflow (64 KiB)
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.num_keys = 4096;
  extra_key_hints.record_size = kKeySize + kValueSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 1024;
  extra_key_hints.key_step_size = 1;
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read all original keys
  std::string value_out;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  // Read all extra keys
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, OverflowByLargeValue) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;
  constexpr size_t kLongValueSize = 504;

  // Dummy value used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);

  // Longer dummy value used for the records (8 byte key; record is 32 KiB in
  // total).
  const std::string value_new(kLongValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // Generate records that will fit in just 1 page.
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Write extra data to the DB.
  llsm::Slice key(reinterpret_cast<const char*>(&(lexicographic_keys[0])),
                  kKeySize);
  status = db->Put(woptions, key, value_new);
  ASSERT_TRUE(status.ok());

  // Flush the write to the pages(shuld cause overflow)
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read updated value
  std::string value_out;
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_new, value_out);

  delete db;
  db = nullptr;
}

TEST_F(DBTest, OverflowWithUpdates) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;

  // Dummy values used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);
  const std::string value_new(kValueSize, 0x00);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // All records fit into 1 page
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Generate data for enough additional writes to definitely overflow (64 KiB)
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.num_keys = 4096;
  extra_key_hints.record_size = kKeySize + kValueSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 1024;
  extra_key_hints.key_step_size = 1;
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Write extra data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages(shuld cause overflow)
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Update some old keys.
  for (size_t i = 0; i < 2; ++i) {
    llsm::Slice key(reinterpret_cast<const char*>(&(lexicographic_keys[i])),
                    kKeySize);
    status = db->Put(woptions, key, value_new);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read all old keys
  std::string value_out;
  for (size_t i = 0; i < lexicographic_keys.size(); ++i) {
    llsm::Slice key(reinterpret_cast<const char*>(&(lexicographic_keys[i])),
                    kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    if (i < 2) {
      ASSERT_EQ(value_new, value_out);
    } else {
      ASSERT_EQ(value_old, value_out);
    }
  }

  // Update some extra keys.
  for (size_t i = extra_lexicographic_keys.size() - 2;
       i < extra_lexicographic_keys.size(); ++i) {
    llsm::Slice key(
        reinterpret_cast<const char*>(&(extra_lexicographic_keys[i])),
        kKeySize);
    status = db->Put(woptions, key, value_new);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read all extra keys
  for (size_t i = 0; i < extra_lexicographic_keys.size(); ++i) {
    llsm::Slice key(
        reinterpret_cast<const char*>(&(extra_lexicographic_keys[i])),
        kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    if (i < extra_lexicographic_keys.size() - 2) {
      ASSERT_EQ(value_old, value_out);
    } else {
      ASSERT_EQ(value_new, value_out);
    }
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, OverflowWithDeletes) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;

  // Dummy values used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);
  const std::string value_new(kValueSize, 0x00);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // All records fit into 1 page
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Generate data for enough additional writes to definitely overflow (64 KiB)
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.num_keys = 4096;
  extra_key_hints.record_size = kKeySize + kValueSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 1024;
  extra_key_hints.key_step_size = 1;
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Delete all original keys
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Delete(llsm::WriteOptions(), key);
    ASSERT_TRUE(status.ok());
  }

  // Flush the deletes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read all extra keys
  std::string value_out;
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  // Reinsert all orginal keys with new value.
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_new);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read all reinserted keys
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_new, value_out);
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, OverflowChain) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;

  // Dummy value used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // All records fit into 1 page
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Generate data for enough additional writes to overflow multiple times (8
  // KiB)
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.num_keys = 512;
  extra_key_hints.record_size = kKeySize + kValueSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 64;
  extra_key_hints.key_step_size = 1;
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read some keys from end of chain
  std::string value_out;
  for (size_t i = extra_lexicographic_keys.size() - 5;
       i < extra_lexicographic_keys.size(); ++i) {
    llsm::Slice key(
        reinterpret_cast<const char*>(&(extra_lexicographic_keys[i])),
        kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, RangeScanOverflow) {
  std::mt19937 rng(42);
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 504;

  // Dummy value used for the records (8 byte key; record is 512 B in total).
  const std::string value(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  // Generate enough records to fit in 2 pages.
  options.key_hints.num_keys = 2 * options.key_hints.records_per_page();
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1000;

  // Generate the keys used in the initial write.
  std::vector<uint64_t> initial_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);
  std::shuffle(initial_keys.begin(), initial_keys.end(), rng);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : initial_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io=*/true);

  // Generate data to overflow the first page.
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.record_size = kKeySize + kValueSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.num_keys = 2 * extra_key_hints.records_per_page();
  extra_key_hints.min_key = 1;
  extra_key_hints.key_step_size = 1;
  std::vector<uint64_t> extra_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);
  std::shuffle(extra_keys.begin(), extra_keys.end(), rng);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  db->FlushMemTable(/*disable_deferred_io=*/true);

  // Extra inserts that we keep in the memtable.
  const uint64_t page0_key = __builtin_bswap64(998ULL);
  const uint64_t page1_key = __builtin_bswap64(99999000ULL);
  status = db->Put(
      woptions,
      llsm::Slice(reinterpret_cast<const char*>(&page0_key), sizeof(page0_key)),
      value);
  ASSERT_TRUE(status.ok());
  status = db->Put(
      woptions,
      llsm::Slice(reinterpret_cast<const char*>(&page1_key), sizeof(page1_key)),
      value);
  ASSERT_TRUE(status.ok());

  // Assemble all the keys and sort them.
  std::vector<uint64_t> all_keys;
  all_keys.reserve(initial_keys.size() + extra_keys.size() + 2);
  all_keys.insert(all_keys.end(), initial_keys.begin(), initial_keys.end());
  all_keys.insert(all_keys.end(), extra_keys.begin(), extra_keys.end());
  all_keys.push_back(page0_key);
  all_keys.push_back(page1_key);
  std::sort(all_keys.begin(), all_keys.end(),
            [](uint64_t left, uint64_t right) {
              return __builtin_bswap64(left) < __builtin_bswap64(right);
            });

  // Read all the records using a single range scan. This scan will include
  // records in the memtable as well as in page chains.
  std::vector<llsm::Record> results;
  status = db->GetRange(
      llsm::ReadOptions(),
      llsm::Slice(reinterpret_cast<const char*>(&(all_keys.front())),
                  sizeof(uint64_t)),
      all_keys.size(), &results);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(results.size(), all_keys.size());

  // Ensure all the records were returned in the correct order.
  for (size_t i = 0; i < results.size(); ++i) {
    llsm::Slice key(reinterpret_cast<const char*>(&(all_keys[i])),
                    sizeof(uint64_t));
    ASSERT_TRUE(key.compare(results[i].key()) == 0);
    ASSERT_EQ(results[i].value(), value);
  }

  delete db;
  db = nullptr;
}

TEST_F(DBTest, ReorgOverflowChain) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;
  constexpr size_t kRecordSize = kKeySize + kValueSize;

  // Dummy value used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kRecordSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // Generate records that will all fit on 1 page.
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Generate data for enough additional writes to overflow multiple times (16
  // KiB)
  constexpr size_t kNumOverflows = 8;
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.record_size = kRecordSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 1024;
  extra_key_hints.key_step_size = 1;
  extra_key_hints.num_keys = kNumOverflows * extra_key_hints.records_per_page();
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  ASSERT_EQ(db->GetNumIndexedPages(), 1);
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read some keys from new pages
  std::string value_out;
  for (size_t i = 0; i < lexicographic_keys.size(); i += 256) {
    llsm::Slice key(reinterpret_cast<const char*>(&(lexicographic_keys[i])),
                    kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }
  for (size_t i = 0; i < extra_lexicographic_keys.size(); i += 256) {
    llsm::Slice key(
        reinterpret_cast<const char*>(&(extra_lexicographic_keys[i])),
        kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  // For reads to have happened, the reorganization must have been completed.
  // This is because reads will block for as long as a reorganization on the
  // page they want to access (the only "old" non-overflow page in this context)
  // is going on.
  ASSERT_EQ(db->GetNumIndexedPages(), 1 + kNumOverflows);

  delete db;
  db = nullptr;
}

TEST_F(DBTest, ReorgOverflowChainNoHint) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;
  constexpr size_t kRecordSize = kKeySize + kValueSize;

  // Dummy value used for the records (8 byte key; record is 16B in total).
  const std::string value_old(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kRecordSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // Generate records that will all fit on 1 page.
  options.key_hints.num_keys = options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);

  // Open the DB.
  llsm::DB* db = nullptr;
  options.key_hints.num_keys = 0;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write dummy data to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages.
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Generate data for enough additional writes to overflow multiple times (16
  // KiB)
  constexpr size_t kNumOverflows = 8;
  llsm::KeyDistHints extra_key_hints;
  extra_key_hints.record_size = kRecordSize;
  extra_key_hints.key_size = kKeySize;
  extra_key_hints.min_key = 1024;
  extra_key_hints.key_step_size = 1;
  extra_key_hints.num_keys = kNumOverflows * extra_key_hints.records_per_page();
  const std::vector<uint64_t> extra_lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(extra_key_hints);

  // Write dummy data to the DB.
  for (const auto& key_as_int : extra_lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    status = db->Put(woptions, key, value_old);
    ASSERT_TRUE(status.ok());
  }

  // Flush the writes to the pages (should cause overflow).
  ASSERT_EQ(db->GetNumIndexedPages(), 1);
  db->FlushMemTable(/*disable_deferred_io = */ true);

  // Read some keys from new pages
  std::string value_out;
  for (size_t i = 0; i < lexicographic_keys.size(); i += 256) {
    llsm::Slice key(reinterpret_cast<const char*>(&(lexicographic_keys[i])),
                    kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }
  for (size_t i = 0; i < extra_lexicographic_keys.size(); i += 256) {
    llsm::Slice key(
        reinterpret_cast<const char*>(&(extra_lexicographic_keys[i])),
        kKeySize);
    status = db->Get(llsm::ReadOptions(), key, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_old, value_out);
  }

  // For reads to have happened, the reorganization must have been completed.
  // This is because reads will block for as long as a reorganization on the
  // page they want to access (the only "old" non-overflow page in this context)
  // is going on.
  ASSERT_EQ(db->GetNumIndexedPages(), 1 + kNumOverflows);

  delete db;
  db = nullptr;
}

TEST_F(DBTest, BulkLoad) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;
  const std::string value(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // Several pages needed for records
  options.key_hints.num_keys = 100 * options.key_hints.records_per_page();

  // Generate data used for the bulk load (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);
  std::vector<std::pair<const llsm::Slice&, const llsm::Slice&>> records;

  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    records.push_back(std::make_pair(key, llsm::Slice(value)));
  }

  // Open the DB without hints.
  options.key_hints.num_keys = 0;
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Bulk load all the records
  llsm::WriteOptions woptions;
  woptions.sorted_load = true;
  status = db->BulkLoad(woptions, records);
  ASSERT_TRUE(status.ok());

  // Read them back
  std::string value_out;
  for (size_t i = 0; i < records.size(); ++i) {
    status = db->Get(llsm::ReadOptions(), records[i].first, &value_out);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, value_out);
  }
}

TEST_F(DBTest, BulkLoadFailureModes) {
  constexpr size_t kKeySize = sizeof(uint64_t);
  constexpr size_t kValueSize = 8;
  const std::string value(kValueSize, 0xFF);

  llsm::Options options;
  options.pin_threads = false;
  options.background_threads = 2;
  options.key_hints.page_fill_pct = 50;
  options.key_hints.record_size = kKeySize + kValueSize;
  options.key_hints.key_size = kKeySize;
  options.key_hints.min_key = 0;
  options.key_hints.key_step_size = 1;
  // Several pages needed for records
  options.key_hints.num_keys = 3 * options.key_hints.records_per_page();

  // Generate data used for the write (and later read).
  const std::vector<uint64_t> lexicographic_keys =
      llsm::key_utils::CreateValues<uint64_t>(options.key_hints);
  std::vector<std::pair<const llsm::Slice&, const llsm::Slice&>> records;

  for (const auto& key_as_int : lexicographic_keys) {
    llsm::Slice key(reinterpret_cast<const char*>(&key_as_int), kKeySize);
    records.push_back(std::make_pair(key, value));
  }

  // Open the DB.
  llsm::DB* db = nullptr;
  llsm::Status status = llsm::DB::Open(options, kDBDir, &db);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(db != nullptr);

  // Write a single record to the DB.
  llsm::WriteOptions woptions;
  woptions.bypass_wal = true;
  status = db->Put(woptions, records[0].first, records[0].second);
  ASSERT_TRUE(status.ok());

  // Fail 1: write options not set right
  status = db->BulkLoad(woptions, records);
  ASSERT_TRUE(status.IsInvalidArgument());

  // Fail 2: a single page exists, but there's already a record there
  woptions.sorted_load = true;
  status = db->BulkLoad(woptions, records);
  ASSERT_TRUE(status.IsNotSupportedError());

  // Write all the records
  for (size_t i = 0; i < records.size(); ++i) {
    status = db->Put(woptions, records[i].first, records[i].second);
  }

  // Fail 3: already several pages there
  status = db->BulkLoad(woptions, records);
  ASSERT_TRUE(status.IsNotSupportedError());
}

}  // namespace
