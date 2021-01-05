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

struct FileAddress {
  size_t file_id;
  size_t offset;
};
FileAddress AddressFromPageId(size_t page_id, llsm::BufMgrOptions options) {
  FileAddress address;
  address.file_id = page_id / options.pages_per_file;
  address.offset = (page_id % options.pages_per_file) * options.page_size;
  return address;
}

// *** Tests ***

TEST(BufferManagerTest, CreateValues) {
  const size_t num_values = 100;
  const std::vector<uint64_t> values = CreateValues<uint64_t>(num_values);
  ASSERT_EQ(values.size(), num_values);
}

TEST(BufferManagerTest, WriteReadSequential) {
  std::string dbpath = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create BufferManager.
  llsm::BufMgrOptions options;
  options.buffer_manager_size = kBufferManagerSize;
  options.num_files = kNumFiles;
  options.pages_per_file = kNumPages / kNumFiles;
  options.page_size = sizeof(size_t);
  llsm::BufferManager buffer_manager(options, dbpath);

  // Store `i` to page i
  for (size_t i = 0; i < kNumPages; ++i) {
    llsm::BufferFrame& bf = buffer_manager.FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager.UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t i = 0; i < kNumPages; ++i) {
    llsm::BufferFrame& bf = buffer_manager.FixPage(i, false);
    ASSERT_EQ(*reinterpret_cast<size_t*>(bf.GetData()), i);
    buffer_manager.UnfixPage(bf, false);
  }

  std::filesystem::remove_all(dbpath);
}

TEST(BufferManagerTest, FlushDirty) {
  std::string dbpath = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create BufferManager.
  llsm::BufMgrOptions options;
  options.buffer_manager_size = kBufferManagerSize;
  options.num_files = kNumFiles;
  options.pages_per_file = kNumPages / kNumFiles;
  options.page_size = sizeof(size_t);
  llsm::BufferManager buffer_manager(options, dbpath);

  // Store `i` to page i for the first kBufferManagerSize pages.
  for (size_t i = 0; i < kBufferManagerSize; ++i) {
    llsm::BufferFrame& bf = buffer_manager.FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager.UnfixPage(bf, true);
  }

  buffer_manager.FlushDirty();

  // Read all pages directly from disk.
  size_t j;
  for (size_t i = 0; i < kBufferManagerSize; ++i) {
    FileAddress address = AddressFromPageId(i, options);
    int fd = open((dbpath + "/segment-" + std::to_string(address.file_id)).c_str(), 
              O_RDWR | O_SYNC | (options.use_direct_io ? O_DIRECT : 0),
              S_IRUSR | S_IWUSR));
    pread(fd, reinterpret_cast<void*>(&j), options.page_size, address.offset);
    close(fd);
    ASSERT_EQ(i, j);
  }

  std::filesystem::remove_all(dbpath);
}

}  // namespace
