#include "rs_model.h"

#include "db/page.h"
#include "util/key.h"

namespace llsm {

// Initalizes the model based on a vector of records sorted by key.
RSModel::RSModel(const KeyDistHints& key_hints,
                 const std::vector<std::pair<Slice, Slice>>& records)
    : records_per_page_(key_hints.records_per_page) {
  // Build RadixSpline.
  const uint64_t min = key_utils::ExtractHead64(records.front().first);
  const uint64_t max = key_utils::ExtractHead64(records.back().first);
  rs::Builder<uint64_t> rsb(min, max);
  for (const auto& record : records)
    rsb.AddKey(key_utils::ExtractHead64(record.first));
  index_ = rsb.Finalize();
}

// Preallocates the number of pages deemed necessary after initialization.
//
// TODO: Add a PageManager to handle this?
void RSModel::Preallocate(const std::vector<std::pair<Slice, Slice>>& records,
                          const std::unique_ptr<BufferManager>& buf_mgr) {
  // Loop over records in records_per_page-sized increments.
  for (size_t record_id = 0; record_id < records.size();
       record_id += records_per_page_) {
    const size_t page_id = record_id / records_per_page_;
    auto& bf = buf_mgr->FixPage(page_id, /*exclusive = */ true);
    const Page page(
        bf.GetData(), records.at(record_id).first,
        records.at(std::min(record_id + records_per_page_, records.size() - 1))
            .first);
    buf_mgr->UnfixPage(bf, /*is_dirty = */ true);
  }
  buf_mgr->FlushDirty();
}

// Uses the model to predict a page_id given a `key` that is within the correct
// range.
size_t RSModel::KeyToPageId(const Slice& key) const {
  return RSModel::KeyToPageId(key_utils::ExtractHead64(key));
}

size_t RSModel::KeyToPageId(const uint64_t key) const {
  const size_t estimate = index_.GetEstimatedPosition(key);

  return estimate / records_per_page_;
}

}  // namespace llsm
