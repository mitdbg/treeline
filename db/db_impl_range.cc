#include <optional>

#include "db/merge_iterators.h"

namespace llsm {

Status DBImpl::GetRange(const ReadOptions& options, const Slice& start_key,
                        const size_t num_records, RecordBatch* results_out) {
  RecordCache::Iterator rc_it = rec_cache_->GetIterator();

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
    is_first_page = false;

    // Merge the record cache results with the page results, prioritizing the
    // record cache for records with the same key.
    while (results_out->size() < num_records && rc_it.Valid() &&
           page_it.Valid()) {
      auto rc_entry = &RecordCache::cache_entries[rc_it.Index()];
      const int compare = rc_entry->GetKey().compare(page_it.key());
      if (compare <= 0) {
        // We only emit the record if it was a write, not a delete.
        if (rc_entry->IsWrite()) {
          results_out->emplace_back(rc_entry->GetKey().ToString(),
                                    rc_entry->GetValue().ToString());
        }
        if (compare == 0) {
          page_it.Next();
        }
        rc_it.Next();
      } else {
        // The page has the smaller record.
        results_out->emplace_back(page_it.key().ToString(),
                                  page_it.value().ToString());
        page_it.Next();
      }
    }

    // This loop only runs if `rc_it` has no remaining records.
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
  // rest of the records in the record cache (if any are left).
  while (results_out->size() < num_records && rc_it.Valid()) {
    auto rc_entry = &RecordCache::cache_entries[rc_it.Index()];
    if (rc_entry->IsWrite()) {
      results_out->emplace_back(rc_entry->GetKey().ToString(),
                                rc_entry->GetValue().ToString());
    }
    rc_it.Next();
  }

  rc_it.Close();
  return Status::OK();
}

}  // namespace llsm
