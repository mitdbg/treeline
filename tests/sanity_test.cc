#include <filesystem>

#include "gtest/gtest.h"
#include "llsm/db.h"

namespace {

TEST(SanityCheck, Create) {
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

TEST(SanityCheck, WriteFlushRead) {
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

  llsm::WriteOptions write_options = {/*sorted_load = */ true,
                                      /*perform_checks = */ false};
  status = db->FlushMemTable(write_options);
  ASSERT_TRUE(status.ok());

  // Should be a page read (but will be cached in the buffer pool).
  status = db->Get(llsm::ReadOptions(), key, &value_out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(value_out, value);

  delete db;
  std::filesystem::remove_all(dbname);
}

}  // namespace
