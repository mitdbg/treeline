#include "bufmgr/buffer_manager.h"

#include <filesystem>
#include <vector>

#include "bufmgr/options.h"
#include "gtest/gtest.h"

// Total number of pages.
const size_t kNumPages = 65536;

// Total number of files used to store the pages.
const size_t kNumFiles = 1;
static_assert(
    kNumPages % kNumFiles == 0,
    "The total number of pages must be divisible by the number of files used.");

// Number of pages in buffer manager.
const size_t kBufferManagerSize = 16384;

namespace {

// *** Helper methods ***

template <typename T>
std::vector<T> CreateValues(const size_t num_values) {
  std::vector<T> values;
  values.reserve(num_values);
  for (size_t i = 0; i < num_values; ++i) values.push_back(i);
  return values;
}

// *** Tests ***

TEST(BufferManagerTest, CreateValues) {
  const size_t num_values = 100;
  const std::vector<uint64_t> values = CreateValues<uint64_t>(num_values);
  ASSERT_EQ(values.size(), num_values);
}

TEST(BufferManagerTest, WriteReadSequential) {
  std::string dbname = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create BufferManager.
  llsm::BufMgrOptions options;
  options.buffer_manager_size = kBufferManagerSize;
  options.num_files = kNumFiles;
  options.pages_per_file = kNumPages / kNumFiles;
  llsm::BufferManager buffer_manager(options, dbname);

  // Store `i` to page i
  for (size_t i = 0; i < kNumPages; ++i) {
    llsm::BufferFrame& bf = buffer_manager.FixPage(i, true);
    reinterpret_cast<uint64_t*>(bf.GetData())[0] = static_cast<uint64_t>(i);
    buffer_manager.UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t i = 0; i < kNumPages; ++i) {
    llsm::BufferFrame& bf = buffer_manager.FixPage(i, false);
    ASSERT_EQ(reinterpret_cast<uint64_t*>(bf.GetData())[0],
              static_cast<uint64_t>(i));
    buffer_manager.UnfixPage(bf, false);
  }

  std::filesystem::remove_all(dbname);
}

}  // namespace
