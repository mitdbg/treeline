#include "bufmgr/file_manager.h"

#include <filesystem>
#include <memory>
#include <vector>

#include "bufmgr/options.h"
#include "gtest/gtest.h"
#include "llsm/options.h"
#include "model/direct_model.h"

namespace {
using namespace llsm;

TEST(FileManagerTest, FileConstruction) {
  std::string dbpath = "/tmp/llsm-filemgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create FileManager.
  llsm::BufMgrOptions buf_mgr_options;
  llsm::Options options;
  std::unique_ptr<DirectModel> model =
      std::make_unique<DirectModel>(options, &buf_mgr_options);
  llsm::FileManager file_manager(buf_mgr_options, dbpath);

  // Check created files.
  for (size_t i = 0; i < buf_mgr_options.num_segments; ++i) {
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
  llsm::BufMgrOptions buf_mgr_options;
  llsm::Options options;
  std::unique_ptr<DirectModel> model =
      std::make_unique<DirectModel>(options, &buf_mgr_options);
  std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(buf_mgr_options, dbpath);
  llsm::FileManager file_manager(buf_mgr_options, dbpath);
  model->Preallocate(buffer_manager);

  // Store `i` to page i
  for (size_t i = 0; i < buf_mgr_options.total_pages; ++i) {
    file_manager.WritePage(i, reinterpret_cast<void*>(&i));
  }

  // Read all pages.
  size_t j;
  void* data = calloc(1, buf_mgr_options.page_size);
  for (size_t i = 0; i < buf_mgr_options.total_pages; ++i) {
    file_manager.ReadPage(i, data);
    j = *reinterpret_cast<size_t*>(data);
    ASSERT_EQ(i, j);
  }
  free(data);
  std::filesystem::remove_all(dbpath);
}

}  // namespace