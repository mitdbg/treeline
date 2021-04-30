#include "bufmgr/buffer_manager.h"

#include <filesystem>
#include <memory>
#include <vector>

#include "db/page.h"
#include "gtest/gtest.h"
#include "llsm/options.h"
#include "model/alex_model.h"
#include "util/key.h"

namespace {

using namespace llsm;

// *** Tests ***

TEST(BufferManagerTest, CreateValues) {
  Options options;
  options.key_hints.num_keys = 100;
  const std::vector<uint64_t> values =
      key_utils::CreateValues<uint64_t>(options.key_hints);
  ASSERT_EQ(values.size(), options.key_hints.num_keys);
}

TEST(BufferManagerTest, WriteReadSequential) {
  const std::string dbname = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data.
  KeyDistHints key_hints;
  key_hints.num_keys = 100000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<ALEXModel>();
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Store `page_id` to page_id
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    llsm::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
    *reinterpret_cast<PhysicalPageId*>(bf.GetData()) = page_id;
    buffer_manager->UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t record_id = 0; record_id < records.size();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    llsm::BufferFrame& bf = buffer_manager->FixPage(page_id, false);
    ASSERT_EQ(*reinterpret_cast<PhysicalPageId*>(bf.GetData()), page_id);
    buffer_manager->UnfixPage(bf, false);
  }

  std::filesystem::remove_all(dbname);
}

TEST(BufferManagerTest, FlushDirty) {
  const std::string dbname = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data.
  KeyDistHints key_hints;
  key_hints.num_keys = 100000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<ALEXModel>();
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Store `page_id` to page_id for the first few pages.
  const size_t few_pages = std::min(
      static_cast<size_t>(3), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t record_id = 0;
       record_id < few_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    llsm::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
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
  const std::string dbname = "/tmp/llsm-bufmgr-test";
  std::filesystem::remove_all(dbname);
  std::filesystem::create_directory(dbname);

  // Create data.
  KeyDistHints key_hints;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Create buffer manager.
  BufMgrOptions bm_options;

  const std::unique_ptr<Model> model = std::make_unique<ALEXModel>();
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->PreallocateAndInitialize(buffer_manager, records,
                                  key_hints.records_per_page());

  // Check that first few pages are contained upon being fixed.
  const size_t few_pages = std::min(
      static_cast<size_t>(3), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t record_id = 0;
       record_id < few_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    ASSERT_FALSE(buffer_manager->Contains(page_id));
    llsm::BufferFrame& bf = buffer_manager->FixPage(page_id, true);
    ASSERT_TRUE(buffer_manager->Contains(page_id));
    buffer_manager->UnfixPage(bf, true);
    ASSERT_TRUE(buffer_manager->Contains(page_id));
  }

  // Check that the following few pages are not contained, having never been
  // fixed.
  const size_t some_more_pages = std::min(
      static_cast<size_t>(6), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t record_id = few_pages * key_hints.records_per_page();
       record_id < some_more_pages * key_hints.records_per_page();
       record_id += key_hints.records_per_page()) {
    PhysicalPageId page_id = model->KeyToPageId(records.at(record_id).first);
    ASSERT_FALSE(buffer_manager->Contains(page_id));
  }

  std::filesystem::remove_all(dbname);
}

}  // namespace
