#include "alex_model.h"

#include <limits>

#include "bufmgr/page_memory_allocator.h"
#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Initalizes the model based on a vector of records sorted by key.
ALEXModel::ALEXModel(const KeyDistHints& key_hints,
                     const std::vector<std::pair<Slice, Slice>>& records)
    : records_per_page_(key_hints.records_per_page()) {}

// Initalizes the model based on existing files, accessed through the `buf_mgr`.
ALEXModel::ALEXModel(const std::unique_ptr<BufferManager>& buf_mgr)
    : records_per_page_(0) {
  const size_t num_segments = buf_mgr->GetFileManager()->GetNumSegments();
  PageBuffer page_data = PageMemoryAllocator::Allocate(/*num_pages=*/1);
  Page temp_page(page_data.get());

  // Loop through files and read each valid page of each file
  for (size_t file_id = 0; file_id < num_segments; ++file_id) {
    for (size_t offset = 0; true; ++offset) {
      PhysicalPageId page_id(file_id, offset);
      if (!buf_mgr->GetFileManager()->ReadPage(page_id, page_data.get()).ok())
        break;

      // Get the first key from the page
      if (!temp_page.IsOverflow()) {
        uint64_t first_key =
            key_utils::ExtractHead64(temp_page.GetLowerBoundary());

        // Insert into index
        index_.insert(first_key, page_id);
      }
    }
  }
}

// Preallocates the number of pages deemed necessary after initialization.
void ALEXModel::Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                            const std::unique_ptr<BufferManager>& buf_mgr) {
  // Loop over records in records_per_page-sized increments.
  for (size_t record_id = 0; record_id < records.size();
       record_id += records_per_page_) {
    const PhysicalPageId page_id = buf_mgr->GetFileManager()->AllocatePage();
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    const Page page(
        bf.GetData(),
        (record_id == 0)
            ? Slice(std::string(records.at(record_id).first.size(), 0x00))
            : records.at(record_id).first,
        (record_id + records_per_page_ < records.size())
            ? records.at(record_id + records_per_page_).first
            : Slice(std::string(records.at(record_id).first.size(), 0xFF)));
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
    index_.insert(key_utils::ExtractHead64(records.at(record_id).first),
                  page_id);
  }
  buf_mgr->FlushDirty();
}

// Uses the model to predict a page_id given a `key` that is within the
// correct range (lower bounds `key`).
PhysicalPageId ALEXModel::KeyToPageId(const Slice& key) {
  return ALEXModel::KeyToPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId ALEXModel::KeyToPageId(const uint64_t key) {
  mutex_.lock_shared();
  PhysicalPageId* page_id_ptr = index_.get_payload_last_no_greater_than(key);
  PhysicalPageId page_id;
  if (page_id_ptr != nullptr) {
    page_id = *page_id_ptr;
  }
  mutex_.unlock_shared();
  return page_id;
}

// Uses the model to predict the page_id of the NEXT page given a `key` that
// is within the correct range (upper bounds `key`). Returns an invalid
// page_id if no next page exists.
PhysicalPageId ALEXModel::KeyToNextPageId(const Slice& key) {
  return ALEXModel::KeyToNextPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId ALEXModel::KeyToNextPageId(const uint64_t key) {
  mutex_.lock_shared();
  PhysicalPageId* page_id_ptr = index_.get_payload_upper_bound(key);
  PhysicalPageId page_id;
  if (page_id_ptr != nullptr) {
    page_id = *page_id_ptr;
  }
  mutex_.unlock_shared();
  return page_id;
}

// Inserts a new mapping into the model (updates the page_id if the key
// already exists).
void ALEXModel::Insert(const Slice& key, const PhysicalPageId& page_id) {
  mutex_.lock();
  index_.insert(key_utils::ExtractHead64(key), page_id);
  mutex_.unlock();
}

// Removes a mapping from the model, if the key exists.
void ALEXModel::Remove(const Slice& key) {
  mutex_.lock();
  index_.erase(key_utils::ExtractHead64(key));
  mutex_.unlock();
}

// Gets the number of pages indexed by the model
size_t ALEXModel::GetNumPages() const { return index_.size(); }

}  // namespace llsm
