#include "bufmgr/buffer_manager.h"

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
  bm_options.SetNumPagesUsing(key_hints);

  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(key_hints, records);
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->Preallocate(records, buffer_manager);

  // Store `i` to page i
  const size_t num_pages = buffer_manager->GetFileManager()->GetNumPages();
  for (size_t i = 0; i < num_pages; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager->UnfixPage(bf, true);
  }

  // Read all pages.
  for (size_t i = 0; i < num_pages; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, false);
    ASSERT_EQ(*reinterpret_cast<size_t*>(bf.GetData()), i);
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
  bm_options.SetNumPagesUsing(key_hints);

  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(key_hints, records);
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->Preallocate(records, buffer_manager);

  // Store `i` to page i for the first few pages.
  const size_t few_pages = std::min(
      static_cast<size_t>(3), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t i = 0; i < few_pages; ++i) {
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, true);
    *reinterpret_cast<size_t*>(bf.GetData()) = i;
    buffer_manager->UnfixPage(bf, true);
  }

  buffer_manager->FlushDirty();

  // Read all pages bypassing buffer manager.
  size_t j;
  void* data = calloc(1, Page::kSize);
  for (size_t i = 0; i < few_pages; ++i) {
    buffer_manager->GetFileManager()->ReadPage(i, data);
    j = *reinterpret_cast<size_t*>(data);
    ASSERT_EQ(i, j);
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
  bm_options.SetNumPagesUsing(key_hints);

  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(key_hints, records);
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(bm_options, dbname);
  model->Preallocate(records, buffer_manager);

  // Check that first few pages are contained upon being fixed.
  const size_t few_pages = std::min(
      static_cast<size_t>(3), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t i = 0; i < few_pages; ++i) {
    ASSERT_FALSE(buffer_manager->Contains(i));
    llsm::BufferFrame& bf = buffer_manager->FixPage(i, true);
    ASSERT_TRUE(buffer_manager->Contains(i));
    buffer_manager->UnfixPage(bf, true);
    ASSERT_TRUE(buffer_manager->Contains(i));
  }

  // Check that the following few pages are not contained, having never been
  // fixed.
  const size_t some_more_pages = std::min(
      static_cast<size_t>(6), buffer_manager->GetFileManager()->GetNumPages());

  for (size_t i = few_pages; i < some_more_pages; ++i) {
    ASSERT_FALSE(buffer_manager->Contains(i));
  }

  std::filesystem::remove_all(dbname);
}

}  // namespace
