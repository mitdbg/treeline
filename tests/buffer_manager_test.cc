#include "bufmgr/buffer_manager.h"

#include <ctime>
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

TEST(BufferManagerTest, CreateValues) {
  Options options;
  options.key_hints.num_keys = 100;
  const std::vector<uint64_t> values =
      key_utils::CreateValues<uint64_t>(options.key_hints);
  ASSERT_EQ(values.size(), options.key_hints.num_keys);
}

TEST(BufferManagerTest, WriteReadSequential) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data.
  KeyDistHints key_hints;
  key_hints.record_size = 512;
  key_hints.num_keys = 10 * key_hints.records_per_page();
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Store `page_id` to page_id
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    tl::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
    *reinterpret_cast<PhysicalPageId*>(bf.GetData()) = page_id;
    buffer_manager->UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    tl::BufferFrame& bf = buffer_manager->FixPage(page_id, false);
    ASSERT_EQ(*reinterpret_cast<PhysicalPageId*>(bf.GetData()), page_id);
    buffer_manager->UnfixPage(bf, false);
  }

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, FlushDirty) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data.
  KeyDistHints key_hints;
  key_hints.record_size = 512;
  key_hints.num_keys = 10 * key_hints.records_per_page();
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Store `page_id` to page_id for the first few pages.
  const size_t few_pages = std::min(
      static_cast<size_t>(3), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t record_id = 0;
       record_id < few_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    tl::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
    *reinterpret_cast<PhysicalPageId*>(bf.GetData()) = page_id;
    buffer_manager->UnfixPage(bf, true);
  }

  buffer_manager->FlushDirty();

  // Read all pages bypassing buffer manager.
  PhysicalPageId j;
  void* data = calloc(1, Page::kSize);
  for (size_t record_id = 0;
       record_id < few_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    buffer_manager->GetFileManager()->ReadPage(page_id, data);
    j = *reinterpret_cast<PhysicalPageId*>(data);
    ASSERT_EQ(page_id, j);
  }
  free(data);
  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, Contains) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);
  // Create data.
  KeyDistHints key_hints;
  key_hints.record_size = 512;
  key_hints.num_keys = 10 * key_hints.records_per_page();
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;
  bm_options.buffer_pool_size = 3 * Page::kSize;
  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Check that first few pages are contained upon being fixed.
  const size_t few_pages = 3;

  for (size_t record_id = 0;
       record_id < few_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    tl::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
    ASSERT_TRUE(buffer_manager->Contains(page_id));
    buffer_manager->UnfixPage(bf, true);
    ASSERT_TRUE(buffer_manager->Contains(page_id));
  }

  // Check that the following few pages are not contained, having never been
  // fixed.
  const size_t some_more_pages = 6;

  for (size_t record_id = few_pages * key_hints.records_per_page();
       record_id < some_more_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    ASSERT_FALSE(buffer_manager->Contains(page_id));
  }

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, IncreaseNumPages) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data and model
  KeyDistHints key_hints;
  key_hints.num_keys = 100000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);
  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();

  // Create buffer manager with 3 pages.
  BufMgrOptions bm_options;
  bm_options.buffer_pool_size = 3 * Page::kSize;
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Fix the first 3 pages.
  std::vector<BufferFrame*> frames;
  for (size_t record_id = 0; record_id < 3 * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    auto bf = &buffer_manager->FixPage(page_id, true);
    frames.push_back(bf);
    ASSERT_TRUE(bf->IsNewlyFixed());
  }

  // Expand cache by 2 pages.
  ASSERT_EQ(buffer_manager->GetNumPages(), 3);
  ASSERT_EQ(buffer_manager->AdjustNumPages(5), 2);
  ASSERT_EQ(buffer_manager->GetNumPages(), 5);

  // Fix another 2 pages; could not have succeeded with the old cache size.
  for (size_t record_id = 3 * key_hints.records_per_page();
       record_id < 5 * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    auto bf = &buffer_manager->FixPage(page_id, true);
    frames.push_back(bf);
    ASSERT_TRUE(bf->IsNewlyFixed());
  }

  // Unfix everything
  for (auto& frame : frames) {
    buffer_manager->UnfixPage(*frame, /*is_dirty = */ false);
  }

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, DecreaseNumPages) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data and model
  KeyDistHints key_hints;
  key_hints.num_keys = 100000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);
  const std::unique_ptr<Model> model = std::make_unique<BTreeModel>();

  // Create buffer manager with 4 pages.
  BufMgrOptions bm_options;
  bm_options.buffer_pool_size = 4 * Page::kSize;
  const std::shared_ptr<BufferManager> buffer_manager =
      std::make_shared<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Fix the first 4 pages.
  std::vector<BufferFrame*> frames;
  for (size_t record_id = 0; record_id < 4 * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    auto bf = &buffer_manager->FixPage(page_id, true);
    frames.push_back(bf);
    ASSERT_TRUE(bf->IsNewlyFixed());
  }

  // Try shrinking cache by 2 pages; can't because nothing is evictable.
  ASSERT_EQ(buffer_manager->GetNumPages(), 4);
  ASSERT_EQ(buffer_manager->AdjustNumPages(2), 0);
  ASSERT_EQ(buffer_manager->GetNumPages(), 4);

  // Unfix the first 2 pages.
  buffer_manager->UnfixPage(*frames.at(0), /*is_dirty = */ false);
  buffer_manager->UnfixPage(*frames.at(1), /*is_dirty = */ false);

  // Try shrinking cache by 2 pages; succeeds in full.
  ASSERT_EQ(buffer_manager->GetNumPages(), 4);
  ASSERT_EQ(buffer_manager->AdjustNumPages(2), -2);
  ASSERT_EQ(buffer_manager->GetNumPages(), 2);

  // Unfix the next page.
  buffer_manager->UnfixPage(*frames.at(2), /*is_dirty = */ false);

  // Try shrinking cache by 2 pages; succeeds in part, only 1 page evictable.
  ASSERT_EQ(buffer_manager->GetNumPages(), 2);
  ASSERT_EQ(buffer_manager->AdjustNumPages(0), -1);
  ASSERT_EQ(buffer_manager->GetNumPages(), 1);

  // Unfix the last page.
  buffer_manager->UnfixPage(*frames.at(3), /*is_dirty = */ false);
  ASSERT_EQ(buffer_manager->GetNumPages(), 1);

  // Used to catch errors in signed -> unsigned integer conversion.
  ASSERT_EQ(buffer_manager->AdjustNumPages(2), 1);
  ASSERT_EQ(buffer_manager->AdjustNumPages(1), -1);

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, FixPageIfFrameAvailable) {
  const std::string dbname =
      "/tmp/tl-bufmgr-test-" + std::to_string(std::time(nullptr));
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create buffer manager with 2 pages.
  BufMgrOptions bm_options;
  bm_options.buffer_pool_size = 2 * Page::kSize;
  std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);

  const auto pg1 = buffer_manager->GetFileManager()->AllocatePage();
  const auto pg2 = buffer_manager->GetFileManager()->AllocatePage();
  const auto pg3 = buffer_manager->GetFileManager()->AllocatePage();

  BufferFrame* const frame1 =
      buffer_manager->FixPageIfFrameAvailable(pg1, /*exclusive=*/false);
  ASSERT_NE(frame1, nullptr);

  BufferFrame* const frame2 =
      buffer_manager->FixPageIfFrameAvailable(pg2, /*exclusive=*/false);
  ASSERT_NE(frame2, nullptr);

  BufferFrame* const frame3 =
      buffer_manager->FixPageIfFrameAvailable(pg3, /*exclusive=*/false);
  ASSERT_EQ(frame3, nullptr);

  buffer_manager->UnfixPage(*frame2, /*is_dirty=*/false);
  BufferFrame* const frame4 =
      buffer_manager->FixPageIfFrameAvailable(pg3, /*exclusive=*/false);
  ASSERT_NE(frame4, nullptr);

  buffer_manager->UnfixPage(*frame1, /*is_dirty=*/false);
  buffer_manager->UnfixPage(*frame4, /*is_dirty=*/false);
  buffer_manager.reset();

  std::filesystem::remove_all(dbname);
}

}  // namespace
