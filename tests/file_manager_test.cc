#include "bufmgr/file_manager.h"

#include <filesystem>
#include <memory>
#include <vector>

#include "db/page.h"
#include "gtest/gtest.h"
#include "llsm/options.h"
#include "model/rs_model.h"
#include "util/key.h"

namespace {

using namespace llsm;

// *** Tests ***

TEST(FileManagerTest, FileConstruction) {
  const std::string dbpath = "/tmp/llsm-filemgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create data.
  KeyDistHints key_hints;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create file manager.
  BufMgrOptions bm_options;
  bm_options.num_segments = 8;
  bm_options.SetNumPagesUsing(key_hints);

  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(key_hints, records);
  const llsm::FileManager file_manager(bm_options, dbpath);

  // Check created files.
  for (size_t i = 0; i < bm_options.num_segments; ++i) {
    ASSERT_TRUE(
        std::filesystem::exists(dbpath + "/segment-" + std::to_string(i)));
  }

  std::filesystem::remove_all(dbpath);
}

TEST(FileManagerTest, WriteReadSequential) {
  const std::string dbpath = "/tmp/llsm-filemgr-test";
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create data.
  KeyDistHints key_hints;
  key_hints.num_keys = 100000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create file manager.
  BufMgrOptions bm_options;
  bm_options.SetNumPagesUsing(key_hints);

  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(key_hints, records);
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbpath);
  llsm::FileManager file_manager(bm_options, dbpath);
  model->Preallocate(records, buffer_manager);

  // Set up a page buffer in memory.
  uint8_t page[Page::kSize];
  memset(page, 0, Page::kSize);

  // Store `i` to page i
  const size_t num_pages = file_manager.GetNumPages();
  for (size_t i = 0; i < num_pages; ++i) {
    *reinterpret_cast<size_t*>(page) = i;
    file_manager.WritePage(i, page);
  }

  // Read pages.
  size_t j;
  void* data = calloc(1, Page::kSize);
  for (size_t i = 0; i < num_pages; ++i) {
    file_manager.ReadPage(i, data);
    j = *reinterpret_cast<size_t*>(data);
    ASSERT_EQ(i, j);
  }
  free(data);
  std::filesystem::remove_all(dbpath);
}

}  // namespace
