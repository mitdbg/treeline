#include "gtest/gtest.h"
#include "llsm/db.h"

TEST(SanityCheck, Create) {
  llsm::DB db;
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
