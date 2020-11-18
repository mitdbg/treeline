#include "gtest/gtest.h"
#include "llsm/db.h"

namespace {

TEST(SanityCheck, Create) {
  llsm::DB* db = nullptr;
  llsm::Options options;
  auto status = llsm::DB::Open(options, "/tmp/llsm-test", &db);
  ASSERT_TRUE(status.IsNotSupportedError());
}

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
