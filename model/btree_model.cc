#include "btree_model.h"

#include <limits>

#include "db/page.h"
#include "util/coding.h"
#include "util/key.h"

namespace llsm {

// Initalizes the model based on a vector of records sorted by key.
BTreeModel::BTreeModel(const KeyDistHints& key_hints,
                     const std::vector<std::pair<Slice, Slice>>& records)
    : records_per_page_(key_hints.records_per_page()) {}

// Initalizes the model based on existing files, accessed through the `buf_mgr`.
BTreeModel::BTreeModel(const std::unique_ptr<BufferManager>& buf_mgr)
    : records_per_page_(0) {
  const size_t num_segments = buf_mgr->GetFileManager()->GetNumSegments();
  char page_data[Page::kSize];
  Page temp_page(reinterpret_cast<void*>(page_data));

  // Loop through files and read each valid page of each file
  for (size_t file_id = 0; file_id < num_segments; ++file_id) {
    for (size_t offset = 0; true; ++offset) {
      PhysicalPageId page_id(file_id, offset);
      if (!buf_mgr->GetFileManager()
               ->ReadPage(page_id, reinterpret_cast<void*>(page_data))
               .ok())
        break;

      // Get the first key from the page
      if (!temp_page.IsOverflow()) {
        uint64_t first_key =
            key_utils::ExtractHead64(temp_page.GetLowerBoundary());

        // Insert into index
        index_.insert2(first_key, page_id);
      }
    }
  }
}

// Preallocates the number of pages deemed necessary after initialization.
//
// TODO: Add a PageManager to handle this?
void BTreeModel::Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                            const std::unique_ptr<BufferManager>& buf_mgr) {
  // Loop over records in records_per_page-sized increments.
  for (size_t record_id = 0; record_id < records.size();
       record_id += records_per_page_) {
    const PhysicalPageId page_id = buf_mgr->GetFileManager()->AllocatePage();
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    const Page page(
        bf.GetData(), records.at(record_id).first,
        records.at(std::min(record_id + records_per_page_, records.size() - 1))
            .first);
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
    index_.insert2(key_utils::ExtractHead64(records.at(record_id).first),
                  page_id);
  }
  buf_mgr->FlushDirty();
}

// Uses the model to predict a page_id given a `key` that is within the
// correct range (lower bounds `key`).
PhysicalPageId BTreeModel::KeyToPageId(const Slice& key) {
  return BTreeModel::KeyToPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId BTreeModel::KeyToPageId(const uint64_t key) {
  auto it = index_.upper_bound(key);
  --it;
  return it->second;
}

// Uses the model to predict the page_id of the NEXT page given a `key` that
// is within the correct range (upper bounds `key`). Returns an invalid
// page_id if no next page exists.
PhysicalPageId BTreeModel::KeyToNextPageId(const Slice& key) {
  return BTreeModel::KeyToNextPageId(key_utils::ExtractHead64(key));
}

PhysicalPageId BTreeModel::KeyToNextPageId(const uint64_t key) {
  auto it = index_.upper_bound(key);
  if (it != index_.end()) {
    return it->second;
  } else {
    return PhysicalPageId();
  }
}

}  // namespace llsm
