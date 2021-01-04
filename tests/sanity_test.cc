#include "gtest/gtest.h"
#include "llsm/db.h"

#include <filesystem>

namespace {

TEST(SanityCheck, Create) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  const std::string dbname = "/tmp/llsm-test";
  std::filesystem::remove_all(dbname);
  auto status = llsm::DB::Open(options, dbname, &db);
  ASSERT_TRUE(status.ok());
  delete db;
  std::filesystem::remove_all(dbname);
}

}  // namespace
