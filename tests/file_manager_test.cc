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
  llsm::Options options;
  const auto values = key_utils::CreateValues<uint64_t>(options.key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Compute the number of records per page.
  const double fill_pct = options.key_hints.page_fill_pct / 100.;
  options.key_hints.records_per_page =
      Page::kSize * fill_pct / options.key_hints.record_size;

  // Create buffer manager.
  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(options.key_hints, records);
  const llsm::FileManager file_manager(options, dbpath);

  // Check created files.
  for (size_t i = 0; i < options.background_threads; ++i) {
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
  llsm::Options options;
  const auto values = key_utils::CreateValues<uint64_t>(options.key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);

  // Compute the number of records per page.
  const double fill_pct = options.key_hints.page_fill_pct / 100.;
  options.key_hints.records_per_page =
      Page::kSize * fill_pct / options.key_hints.record_size;

  // Create file manager.
  const std::unique_ptr<RSModel> model =
      std::make_unique<RSModel>(options.key_hints, records);
  const std::unique_ptr<BufferManager> buffer_manager =
      std::make_unique<BufferManager>(options, dbpath);
  llsm::FileManager file_manager(options, dbpath);
  model->Preallocate(records, buffer_manager);

  // Store `i` to page i
  const size_t num_pages = file_manager.GetNumPages();
  for (size_t i = 0; i < num_pages; ++i) {
    file_manager.WritePage(i, reinterpret_cast<void*>(&i));
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
