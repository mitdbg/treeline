#include "llsm/db.h"

#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>

#include "db/page.h"
#include "gtest/gtest.h"

namespace {

bool EqualTimespec(const timespec& lhs, const timespec& rhs) {
  return (lhs.tv_sec == rhs.tv_sec) && (lhs.tv_nsec == rhs.tv_nsec);
}

TEST(DBTest, Create) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  // The test environment may not have many cores.
  options.pin_threads = false;
  options.num_keys = 10;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
  ASSERT_TRUE(status.ok());
  delete db;
  std::filesystem::remove_all(dbname);
}

TEST(DBTest, WriteFlushRead) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.num_keys = 10;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
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

  llsm::FlushOptions flush_options = {/*disable_deferred_io = */ true};
  status = db->FlushMemTable(flush_options);
  ASSERT_TRUE(status.ok());

  // Should be a page read (but will be cached in the buffer pool).
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
  std::filesystem::remove_all(dbname);
}

TEST(DBTest, WriteThenDelete) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.num_keys = 10;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
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
  llsm::FlushOptions flush_options = {/*disable_deferred_io = */ true};
  status = db->FlushMemTable(flush_options);
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
  status = db->FlushMemTable(flush_options);
  ASSERT_TRUE(status.ok());

  // Delete
  status = db->Delete(llsm::WriteOptions(), key3);
  ASSERT_TRUE(status.ok());

  // Flush
  status = db->FlushMemTable(flush_options);
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
  status = db->FlushMemTable(flush_options);
  ASSERT_TRUE(status.ok());

  // Should not find it.
  status = db->Get(llsm::ReadOptions(), key4, &value_out);
  ASSERT_TRUE(status.IsNotFound());

  delete db;
  std::filesystem::remove_all(dbname);
}

TEST(DBTest, DeferByEntries) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.num_keys = 10;
  options.record_size = 16 * 1024;  // 4 per page
  options.page_fill_pct = 100;
  options.deferred_io_min_entries = 2;
  options.deferred_io_max_deferrals = 4;
  options.buffer_pool_size = llsm::Page::kSize;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
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
  auto filename = dbname + "/segment-0";
  timespec mod_time;
  struct stat result;
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    mod_time = result.st_mtim;
  }

  // Flush - shouldn't flush anything
  status = db->FlushMemTable(llsm::FlushOptions());
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
  status = db->FlushMemTable(llsm::FlushOptions());
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
  std::filesystem::remove_all(dbname);
}

TEST(DBTest, DeferByAttempts) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  options.pin_threads = false;
  options.num_keys = 10;
  options.record_size = 16 * 1024;  // 4 per page
  options.page_fill_pct = 100;
  options.deferred_io_min_entries = 2;
  options.deferred_io_max_deferrals = 1;
  options.buffer_pool_size = llsm::Page::kSize;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
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
  auto filename = dbname + "/segment-0";
  timespec mod_time;
  struct stat result;
  sync();
  if (stat(filename.c_str(), &result) == 0) {
    mod_time = result.st_mtim;
  }

  // Flush - shouldn't flush anything
  status = db->FlushMemTable(llsm::FlushOptions());
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
  status = db->FlushMemTable(llsm::FlushOptions());
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
  std::filesystem::remove_all(dbname);
}
}  // namespace
