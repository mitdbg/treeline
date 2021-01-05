#include "bufmgr/file_manager.h"

#include <filesystem>
#include <vector>

#include "bufmgr/options.h"
#include "gtest/gtest.h"

// Total number of pages.
const size_t kNumPages = 65536;

// Total number of files used to store the pages.
const size_t kNumFiles = 8;
static_assert(
    kNumPages % kNumFiles == 0,
    "The total number of pages must be divisible by the number of files used.");

// Number of pages in buffer manager.
const size_t kBufferManagerSize = 16384;

namespace {

TEST(FileManagerTest, FileConstruction) {
  std::string dbpath = "/tmp/llsm-filemgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create FileManager.
  llsm::BufMgrOptions options;
  options.buffer_manager_size = kBufferManagerSize;
  options.num_files = kNumFiles;
  options.pages_per_file = kNumPages / kNumFiles;
  llsm::FileManager file_manager(options, dbpath);

  // Check created files.
  for (size_t i = 0; i < options.num_files; ++i) {
    ASSERT_TRUE(
        std::filesystem::exists(dbpath + "/segment-" + std::to_string(i)));
  }

  std::filesystem::remove_all(dbpath);
}

TEST(FileManagerTest, WriteReadSequential) {
  std::string dbpath = "/tmp/llsm-filemgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create FileManager.
  llsm::BufMgrOptions options;
  options.buffer_manager_size = kBufferManagerSize;
  options.num_files = kNumFiles;
  options.pages_per_file = kNumPages / kNumFiles;
  llsm::FileManager file_manager(options, dbpath);

  // Store `i` to page i
  for (size_t i = 0; i < kNumPages; ++i) {
    file_manager.WritePage(i, reinterpret_cast<void*>(&i));
  }

  // Read all pages.
  size_t j;
  for (size_t i = 0; i < kNumPages; ++i) {
    file_manager.ReadPage(i, reinterpret_cast<void*>(&j));
    ASSERT_EQ(i, j);
  }

  std::filesystem::remove_all(dbpath);
}

}  // namespace