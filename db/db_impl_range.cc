#include <optional>

#include "db/merge_iterators.h"

namespace llsm {

Status DBImpl::GetRange(const ReadOptions& options, const Slice& start_key,
                        const size_t num_records, RecordBatch* results_out) {
  // We need hold a copy of the memtable shared pointers to ensure the memtables
  // do not get deleted while we do the scan.
  std::shared_ptr<MemTable> local_mtable = nullptr;
  std::shared_ptr<MemTable> local_im_mtable = nullptr;
  {
    std::unique_lock<std::mutex> mtable_lock(mtable_mutex_);
    local_mtable = mtable_;
    local_im_mtable = im_mtable_;
  }

  MemTable::Iterator active = local_mtable->GetIterator();
  std::optional<MemTable::Iterator> immutable =
      local_im_mtable != nullptr ? local_im_mtable->GetIterator()
                                 : std::optional<MemTable::Iterator>();

  MemTableMergeIterator mtable_it(local_mtable, local_im_mtable, start_key);

  results_out->clear();
  results_out->reserve(num_records);

  OverflowChain curr_page_chain = nullptr;
  PhysicalPageId curr_page_id = model_->KeyToPageId(start_key);
  bool is_first_page = true;

  while (results_out->size() < num_records && curr_page_id.IsValid()) {
    // If we had already fixed a chain previously, we want to keep it fixed
    // until we successfully fix the next chain. Otherwise, there is a risk of
    // the previous chain being reorganized before we can fix the next chain.
    OverflowChain prev_page_chain(std::move(curr_page_chain));

    while (curr_page_id.IsValid()) {
      curr_page_chain = FixOverflowChain(curr_page_id, /*exclusive=*/false,
                                         /*unlock_before_returning=*/false);
      if (curr_page_chain != nullptr) break;

      // Query the model for the page ID again because it may have changed due
      // to reorganization.
      if (is_first_page) {
        curr_page_id = model_->KeyToPageId(start_key);
      } else {
        assert(prev_page_chain != nullptr);
        curr_page_id = model_->KeyToNextPageId(
            prev_page_chain->at(0)->GetPage().GetLowerBoundary());
      }
    }

    // We have fixed the "next" page chain (or will exit this loop), so now it's
    // safe to unfix the previous page chain.
    if (prev_page_chain != nullptr) {
      assert(!is_first_page);
      for (auto& bf : *prev_page_chain) {
        buf_mgr_->UnfixPage(*bf, /*is_dirty=*/false);
      }
      prev_page_chain = nullptr;
    }

    // This is a defensive check - we currently don't "shrink" the number of
    // pages during reorganization (e.g., because of deletes), so `curr_page_id`
    // should remain valid even if we had queried the model again (in the while
    // loop above).
    if (!curr_page_id.IsValid()) {
      break;
    }

    PageMergeIterator page_it(curr_page_chain,
                              is_first_page ? &start_key : nullptr);
    if (is_first_page) {
      is_first_page = false;
    }

    // Merge the memtable results with the page results, prioritizing the
    // memtable(s) for records with the same key.
    while (results_out->size() < num_records && mtable_it.Valid() &&
           page_it.Valid()) {
      const int compare = mtable_it.key().compare(page_it.key());
      if (compare <= 0) {
        // We do not emit the record if it was deleted.
        if (mtable_it.type() == format::WriteType::kWrite) {
          results_out->emplace_back(mtable_it.key().ToString(),
                                    mtable_it.value().ToString());
        }
        if (compare == 0) {
          page_it.Next();
        }
        mtable_it.Next();

      } else {
        // The page has the smaller record.
        results_out->emplace_back(page_it.key().ToString(),
                                  page_it.value().ToString());
        page_it.Next();
      }
    }

    // This loop only runs if `mtable_it` has no remaining records.
    while (results_out->size() < num_records && page_it.Valid()) {
      results_out->emplace_back(page_it.key().ToString(),
                                page_it.value().ToString());
      page_it.Next();
    }

    // Find the next page chain we should load.
    curr_page_id = model_->KeyToNextPageId(
        curr_page_chain->at(0)->GetPage().GetLowerBoundary());
  }

  // Unfix the last chain that we processed in the loop above.
  if (curr_page_chain != nullptr) {
    for (auto& bf : *curr_page_chain) {
      buf_mgr_->UnfixPage(*bf, /*is_dirty=*/false);
    }
    curr_page_chain = nullptr;
  }

  // No more pages to check. If we still need to read more records, read the
  // rest of the records in the memtable(s) (if any are left).
  while (results_out->size() < num_records && mtable_it.Valid()) {
    if (mtable_it.type() == format::WriteType::kWrite) {
      results_out->emplace_back(mtable_it.key().ToString(),
                                mtable_it.value().ToString());
    }
    mtable_it.Next();
  }

  return Status::OK();
}

}  // namespace llsm
