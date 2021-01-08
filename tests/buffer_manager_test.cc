#include "bufmgr/buffer_manager.h"

#include <filesystem>
#include <memory>
#include <vector>

#include "bufmgr/options.h"
#include "gtest/gtest.h"
#include "llsm/options.h"
#include "model/direct_model.h"

namespace {

using namespace llsm;

// *** Helper methods ***

template <typename T>
std::vector<T> CreateValues(const size_t num_values) {
  std::vector<T> values;
  values.reserve(num_values);
  for (size_t i = 0; i < num_values; ++i) values.push_back(i);
  return values;
}

// Uses the model to derive a FileAddress given a `page_id`.
FileAddress PageIdToAddress(const size_t page_id,
                            const size_t pages_per_segment,
                            const size_t page_size) {
  FileAddress address;
  address.file_id = page_id / pages_per_segment;
  address.offset = (page_id % pages_per_segment) * page_size;
  return address;
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
  llsm::BufMgrOptions buf_mgr_options;
  llsm::Options options;
  std::unique_ptr<DirectModel> model =
      std::make_unique<DirectModel>(options, &buf_mgr_options);
  std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(buf_mgr_options, dbname);
  model->Preallocate(buffer_manager);

  // Store `i` to page i
  for (size_t i = 0; i < buf_mgr_options.total_pages; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager->UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t i = 0; i < buf_mgr_options.total_pages; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, false);
    ASSERT_EQ(*reinterpret_cast<size_t*>(bf.GetData()), i);
    buffer_manager->UnfixPage(bf, false);
  }

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, FlushDirty) {
  std::string dbname = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create BufferManager.
  llsm::BufMgrOptions buf_mgr_options;
  llsm::Options options;
  std::unique_ptr<DirectModel> model =
      std::make_unique<DirectModel>(options, &buf_mgr_options);
 std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(buf_mgr_options, dbname);
  model->Preallocate(buffer_manager);

  // Store `i` to page i for the first few pages.
  for (size_t i = 0; i < 3; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager->UnfixPage(bf, true);
  }

  buffer_manager->FlushDirty();

  // Read all pages directly from disk.
  size_t j;
  void* data = calloc(1, buf_mgr_options.page_size);
  for (size_t i = 0; i < 3; ++i) {
    llsm::FileAddress address = PageIdToAddress(
        i, buf_mgr_options.pages_per_segment, buf_mgr_options.page_size);
    int fd =
        open((dbname + "/segment-" + std::to_string(address.file_id)).c_str(),
             O_RDWR | O_SYNC | (buf_mgr_options.use_direct_io ? O_DIRECT : 0),
             S_IRUSR | S_IWUSR);
    pread(fd, data, buf_mgr_options.page_size, address.offset);
    close(fd);
    j = *reinterpret_cast<size_t*>(data);
    ASSERT_EQ(i, j);
  }
  free(data);
  std::filesystem::remove_all(dbname);
}

}  // namespace
