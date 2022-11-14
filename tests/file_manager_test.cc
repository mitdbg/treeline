#include "bufmgr/file_manager.h"

#include <filesystem>
#include <memory>
#include <vector>

#include "db/page.h"
#include "gtest/gtest.h"
#include "treeline/options.h"
#include "model/btree_model.h"
#include "util/key.h"

namespace {

using namespace tl;

// *** Tests ***

TEST(FileManagerTest, FileConstruction) {
  const std::string dbpath =
      "/tmp/tl-filemgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create data.
  KeyDistHints key_hints;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create file manager.
  BufMgrOptions bm_options;
  bm_options.num_segments = 8;

  const tl::FileManager file_manager(bm_options, dbpath);

  // Check created files.
  for (size_t i = 0; i < bm_options.num_segments; ++i) {
    ASSERT_TRUE(
        std::filesystem::exists(dbpath + "/segment-" + std::to_string(i)));
  }

  std::filesystem::remove_all(dbpath);
}

TEST(FileManagerTest, WriteReadSequential) {
  const std::string dbpath =
      "/tmp/tl-filemgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbpath);
  std::filesystem::create_directory(dbpath);

  // Create data.
  KeyDistHints key_hints;
  key_hints.record_size = 512;
  key_hints.num_keys = 10 * key_hints.records_per_page();
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create file manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbpath);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Set up a page buffer in memory.
  uint8_t page[Page::kSize];
  memset(page, 0, Page::kSize);

  // Store `page_id` to page_id
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    *reinterpret_cast<PhysicalPageId*>(page) = page_id;
    buffer_manager->GetFileManager()->WritePage(page_id, page);
  }

  // Read all pages.
  PhysicalPageId j;
  void* data = calloc(1, Page::kSize);
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    buffer_manager->GetFileManager()->ReadPage(page_id, data);
    j = *reinterpret_cast<PhysicalPageId*>(data);
    ASSERT_EQ(page_id, j);
  }
  free(data);
  std::filesystem::remove_all(dbpath);
}

}  // namespace
