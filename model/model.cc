#include "model/model.h"

#include "db/page.h"

namespace tl {

void Model::PreallocateAndInitialize(
    const std::shared_ptr<BufferManager>& buf_mgr,
    const std::vector<std::pair<Slice, Slice>>& records,
    const size_t records_per_page) {
  // Edge case: empty database. Allocate one page for the entire key range.
  if (records.size() == 0) {
    const PhysicalPageId page_id = buf_mgr->GetFileManager()->AllocatePage();
    auto& bf = buf_mgr->FixPage(page_id, /* exclusive = */ true,
                                /* is_newly_allocated = */ true);
    const Page page(
        bf.GetData(), Slice(std::string(1, 0x00)),
        key_utils::IntKeyAsSlice(std::numeric_limits<uint64_t>::max())
            .as<Slice>());
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
    this->Insert(Slice(std::string(1, 0x00)), page_id);
  }

  // Loop over records in records_per_page-sized increments.
  for (size_t record_id = 0; record_id < records.size();
       record_id += records_per_page) {
    const PhysicalPageId page_id = buf_mgr->GetFileManager()->AllocatePage();
    auto& bf = buf_mgr->FixPage(page_id, /* exclusive = */ true,
                                /* is_newly_allocated = */ true);
    const Page page(
        bf.GetData(),
        (record_id == 0) ? Slice(std::string(1, 0x00))
                         : records.at(record_id).first,
        (record_id + records_per_page < records.size())
            ? records.at(record_id + records_per_page).first
            : key_utils::IntKeyAsSlice(std::numeric_limits<uint64_t>::max())
                  .as<Slice>());
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
    this->Insert(records.at(record_id).first, page_id);
  }
  buf_mgr->FlushDirty();
}

void Model::ScanFilesAndInitialize(
    const std::shared_ptr<BufferManager>& buf_mgr) {
  const size_t num_segments = buf_mgr->GetFileManager()->GetNumSegments();
  PageBuffer page_data = PageMemoryAllocator::Allocate(/*num_pages=*/1);
  Page temp_page(page_data.get());

  // Loop through files and read each valid page of each file
  for (size_t file_id = 0; file_id < num_segments; ++file_id) {
    for (size_t offset = 0; true; ++offset) {
      PhysicalPageId page_id(file_id, offset);
      if (!buf_mgr->GetFileManager()->ReadPage(page_id, page_data.get()).ok())
        break;

      // If not an overflow page, insert lower boundary into index
      if (!temp_page.IsOverflow() && temp_page.IsValid()) {
        this->Insert(temp_page.GetLowerBoundary(), page_id);
      }
    }
  }
}

}  // namespace tl
