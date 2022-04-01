#include "overflow_chain.h"

#include <inttypes.h>

#include <tuple>

#include "bufmgr/page_memory_allocator.h"
#include "db/logger.h"
#include "db/merge_iterators.h"

namespace tl {

OverflowChain FixOverflowChain(PhysicalPageId page_id, bool exclusive,
                               bool unlock_before_returning,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model,
                               Statistics* stats) {
  BufferFrame* bf;
  PhysicalPageId local_page_id = page_id;

  // Fix first page and check for reorganization
  const size_t pages_before = model->GetNumPages();
  bf = &(buf_mgr->FixPage(local_page_id, exclusive));
  const size_t pages_after = model->GetNumPages();

  if (pages_before != pages_after) {
    buf_mgr->UnfixPage(*bf, /*is_dirty=*/false);
    return nullptr;
  }

  if (bf->IsNewlyFixed()) {
    ++(stats->temp_flush_bufmgr_misses_pages_);
  } else {
    ++(stats->temp_flush_bufmgr_hits_pages_);
  }

  OverflowChain frames = std::make_unique<std::vector<BufferFrame*>>();
  frames->push_back(bf);

  // Fix all overflow pages in the chain, one by one. This loop will "back
  // off" and retry if it finds out that there are not enough free frames in
  // the buffer pool to fix all the pages in the chain.
  //
  // Note that it is still possible for this loop to cause a livelock if all
  // threads end up fixing and unfixing their pages in unison.
  while (frames->back()->GetPage().HasOverflow()) {
    local_page_id = frames->back()->GetPage().GetOverflow();
    bf = buf_mgr->FixPageIfFrameAvailable(local_page_id, exclusive);
    if (bf == nullptr) {
      // No frames left. We should unfix all overflow pages in this chain
      // (i.e., all pages except the first page in the chain) to give other
      // workers a chance to fix their entire chain.
      while (frames->size() > 1) {
        buf_mgr->UnfixPage(*(frames->back()), /*is_dirty=*/false);
        frames->pop_back();
      }
      assert(frames->size() == 1);
      // Retry from the beginning of the chain.
      continue;
    }

    if (bf->IsNewlyFixed()) {
      ++stats->temp_flush_bufmgr_misses_pages_;
    } else {
      ++stats->temp_flush_bufmgr_hits_pages_;
    }
    frames->push_back(bf);
  }

  if (unlock_before_returning) {
    for (auto& bf : *frames) {
      // Unlock the frame so that it can be "handed over" to the caller. This
      // does not decrement the fix count of the frame, i.e. there's no danger
      // of eviction before we can use it.
      bf->Unlock();
    }
  }

  return frames;
}

// Notes on concurrent operations during reorganization
//
// Concurrent writers:
//     --  Only RecordCache::WriteOutIfDirty() writes to pages, which uses
//         FixOverflowChain() to get an OverflowChain. That call of
//         FixOverflowChain() will serialize with this function's call to
//         FixOverflowChain(), since exactly one of the calls will manage to
//         lock the first chain link first.
//     --  If RecordCache::WriteOutIfDirty()'s call goes first, we'll block
//         until the flush completes and follow afterwards.
//     --  If our call goes first, RecordCache::WriteOutIfDirty()'s call will
//         block until we are done reorganizing and then re-query the model.
//
// Concurrent readers:
//     --  Any readers that already had a non-exclusive lock on some
//         page in the chain will proceed normally, since we have to wait for
//         them to finish in order for us to return from FixOverflowChain().
//     --  Any readers that haven't fixed the first link of the chain yet will
//         block in FixPage() until we are done and then re-consult the model,
//         where they might discover that they need to try again (see
//         DBImpl::Get() step 2).
//
//
// TODO: Some of the assumptions about space usage below will need to be thought
// out more carefully for dealing with variable-sized records and deletions.
//
Status ReorganizeOverflowChain(PhysicalPageId page_id, uint32_t page_fill_pct,
                               std::shared_ptr<BufferManager> buf_mgr,
                               std::shared_ptr<Model> model, Options* options,
                               Statistics* stats) {
  OverflowChain chain = nullptr;
  while (chain == nullptr) {
    chain = FixOverflowChain(page_id, /* exclusive = */ true,
                             /* unlock_before_returning = */ false, buf_mgr,
                             model, stats);
  }

  // Avoid accidental extra work if we scheduled the reorganization twice.
  if (chain->size() == 1) {
    buf_mgr->UnfixPage(*(chain->at(0)), /* is_dirty = */ false);
    return Status::OK();
  }

  // Check that chain is reorganizable within our constraints.
  if (chain->size() > options->max_reorg_fanout) {
    for (auto& frame : *chain) {
      buf_mgr->UnfixPage(*frame, /* is_dirty = */ false);
    }

    Logger::Log(
        "Chain is too long to be reorganized without violating the maximum "
        "reorganization fanout. Chain length: %zu",
        chain->size());

    return Status::InvalidArgument(
        "Chain is too long to be reorganized without violating the maximum "
        "reorganization fanout.");
  }

  KeyDistHints dist;
  // All records in a chain currently share a prefix, so they will share a
  // prefix that is at least as long after reorganization. This reduces their
  // effective size.
  const size_t full_record_size = options->key_hints.record_size;
  const size_t effective_record_size =
      full_record_size - chain->at(0)->GetPage().GetKeyPrefix().size();
  dist.record_size = effective_record_size;
  dist.key_size = options->key_hints.key_size;
  dist.page_fill_pct = page_fill_pct;

  // This is a conservative estimate for the number of keys, based on the
  // assumption that every page in the chain is full of records of size
  // `effective_record_size` and no additional common prefix compression will
  // be feasible for any post-reorganization page, beyond that present in the
  // original chain.
  //
  // We need this estimate to calculate `dist.num_pages()` below and adjust
  // the fill percentage as needed.
  //
  // The estimate is refined later on, after we go over the records and count
  // them.
  dist.num_keys = chain->size() *
                  ((Page::UsableSize() -
                    2 * full_record_size) /  // Subtract space used for fences.
                   (effective_record_size + Page::PerRecordMetadataSize()));

  while (dist.num_pages() > options->max_reorg_fanout) {
    ++dist.page_fill_pct;
    // This won't exceed 100% because at some point we will hit the equivalent
    // per-page fullness that corresponds to the current chain length, which we
    // know is not longer than options_.max_reorg_fanout.
    //
    // For example if the chain has 3 links with fullness 100%, 100%, 40%, this
    // loop will stop once we hit 80%, because that amount of total space (3
    // pages, each 80% full) is used by the current chain.
  }
  const size_t records_per_page = dist.records_per_page();

  // 1. First pass to find boundaries and count number of records
  size_t record_count = 0;
  std::vector<std::string> boundary_keys;
  PageMergeIterator pmi(chain);
  boundary_keys.emplace_back(
      chain->at(0)->GetPage().GetLowerBoundary().ToString());

  while (pmi.Valid()) {
    if (record_count % records_per_page == 0 && record_count > 0)
      boundary_keys.emplace_back(pmi.key().ToString());
    ++record_count;
    pmi.Next();
  }

  assert(record_count <= dist.num_keys);  // Sanity check
  dist.num_keys = record_count;
  boundary_keys.emplace_back(
      chain->at(0)->GetPage().GetUpperBoundary().ToString());
  // All pages in the chain have the same lower/upper boundary, since
  // they inherit it from the previous chain link upon construction.
  // The lower boundary is the smallest key that could go into this overflow
  // chain, the upper boundary is the smallest key that would go into the *next*
  // page through the model. Together, they define the common prefix of all the
  // keys in this overflow chain.

  // 2. Allocate and initialize in-memory pages
  const size_t old_num_pages = chain->size();
  const size_t new_num_pages = dist.num_pages();
  assert(boundary_keys.size() == new_num_pages + 1);
  PageBuffer page_data = PageMemoryAllocator::Allocate(new_num_pages);

  std::vector<Page> pages;
  for (size_t i = 0; i < new_num_pages; ++i) {
    pages.emplace_back(page_data.get() + i * Page::kSize, boundary_keys.at(i),
                       boundary_keys.at(i + 1));
  }

  // 3. Populate in-memory pages
  size_t temp_record_count = 0;
  PageMergeIterator pmi2(chain);

  while (pmi2.Valid()) {
    pages.at(temp_record_count / records_per_page)
        .Put(pmi2.key(), pmi2.value());
    ++temp_record_count;
    pmi2.Next();
  }

  // 4. Update data and model, adding new pages to chain as required.
  // Do this backwards to ensure correct behavior for stalled reads (i.e. ensure
  // that they will wait for entire reorg to complete).
  for (size_t i = new_num_pages - 1; i < new_num_pages; --i) {
    BufferFrame* frame;
    if (i < old_num_pages) {
      frame = chain->at(i);
    } else {
      PhysicalPageId new_page_id = buf_mgr->GetFileManager()->AllocatePage();
      frame = &(buf_mgr->FixPage(new_page_id, /* exclusive = */ true,
                                 /* is_newly_allocated = */ true));
    }

    memcpy(frame->GetData(), page_data.get() + i * Page::kSize, Page::kSize);
    model->Insert(frame->GetPage().GetLowerBoundary(), frame->GetPageId());
    // No need to remove anything from the model; the lower boundary of the
    // first page will simply be overwritten.

    buf_mgr->UnfixPage(*frame, /* is_dirty = */ true);
  }

  // This only runs if there are more old pages than new pages. The older pages
  // will be leaked on disk because no other pages reference them. This is a
  // defensive check because, in theory, we should never produce fewer pages
  // compared to what was originally in the chain.
  //
  // N.B. This can in fact happen with deletions or with updates to some key
  // using a larger value than the original. Our code might need more changes to
  // efficiently handle these scenaria.
  for (size_t i = new_num_pages; i < old_num_pages; ++i) {
    memset(chain->at(i)->GetData(), 0, Page::kSize);
    buf_mgr->UnfixPage(*(chain->at(i)), /*is_dirty=*/true);
  }
  if (new_num_pages < old_num_pages) {
    uint64_t lower = 0, upper = 0;
    lower = *reinterpret_cast<const uint64_t*>(boundary_keys.front().data());
    if (!boundary_keys.back().empty()) {
      upper = *reinterpret_cast<const uint64_t*>(boundary_keys.back().data());
    }
    lower = __builtin_bswap64(lower);
    upper = __builtin_bswap64(upper);
    Logger::Log(
        "WARNING: Reorganization produced fewer pages than the length of the "
        "original chain. Pages will be leaked on disk.\nOld Pages: %zu"
        ", New Pages: %zu\nChain Boundary Lower: %" PRIu64
        ", Chain Boundary Upper: %" PRIu64,
        old_num_pages, new_num_pages, lower, upper);
  }

  ++(stats->reorg_count_);
  stats->reorg_pre_total_length_ += old_num_pages;
  stats->reorg_post_total_length_ += new_num_pages;

  return Status::OK();
}

// Reorganizes the page chain `chain` so as to efficiently insert `records`.
Status PreorganizeOverflowChain(const FlushBatch& records, OverflowChain chain,
                                uint32_t page_fill_pct,
                                std::shared_ptr<BufferManager> buf_mgr,
                                std::shared_ptr<Model> model, Options* options,
                                Statistics* stats) {
  // 1. Merge the FlushBatch with the records currently in the chain to have
  // everything in sorted order.
  RecordBatch old_records;
  ExtractOldRecords(chain, &old_records);
  FlushBatch merged;
  MergeBatches(old_records, records, &merged);

  // 2. Calculate records per page.
  KeyDistHints dist;
  dist.num_keys = merged.size();
  dist.key_size =
      std::get<0>(records[0]).size() -
      chain->at(0)->GetPage().GetKeyPrefix().size();  // Assume same size.
  dist.record_size = dist.key_size + std::get<1>(records[0]).size();
  dist.page_fill_pct = page_fill_pct;
  while (dist.num_pages() > options->max_reorg_fanout) {
    ++dist.page_fill_pct;  // TODO: Think if this might create issues.
  }
  const size_t records_per_page = dist.records_per_page();

  // 3. Allocate and populate in-memory pages
  const size_t old_num_pages = chain->size();
  const size_t new_num_pages = dist.num_pages();
  PageBuffer page_data = PageMemoryAllocator::Allocate(new_num_pages);

  std::vector<Page> pages;
  Slice old_lower_boundary = chain->at(0)->GetPage().GetLowerBoundary();
  Slice old_upper_boundary = chain->at(0)->GetPage().GetUpperBoundary();
  for (size_t i = 0; i < new_num_pages; ++i) {
    pages.emplace_back(page_data.get() + i * Page::kSize,
                       (i == 0) ? old_lower_boundary
                                : std::get<0>(merged[i * records_per_page]),
                       (i == (new_num_pages - 1))
                           ? old_upper_boundary
                           : std::get<0>(merged[(i + 1) * records_per_page]));
  }
  for (size_t i = 0; i < merged.size(); ++i) {
    pages.at(i / records_per_page)
        .Put(std::get<0>(merged[i]), std::get<1>(merged[i]));
  }

  // 4. Update data and model, adding new pages to chain as required.
  // Do this backwards to ensure correct behavior for stalled reads (i.e. ensure
  // that they will wait for entire reorg to complete).
  for (size_t i = new_num_pages - 1; i < new_num_pages; --i) {
    BufferFrame* frame;
    if (i < old_num_pages) {
      frame = chain->at(i);
    } else {
      PhysicalPageId new_page_id = buf_mgr->GetFileManager()->AllocatePage();
      frame = &(buf_mgr->FixPage(new_page_id, /* exclusive = */ true,
                                 /* is_newly_allocated = */ true));
    }

    memcpy(frame->GetData(), page_data.get() + i * Page::kSize, Page::kSize);
    model->Insert(frame->GetPage().GetLowerBoundary(), frame->GetPageId());
    // No need to remove anything from the model; the lower boundary of the
    // first page will simply be overwritten.

    buf_mgr->UnfixPage(*frame, /* is_dirty = */ true);
  }

  // This only runs if there are more old pages than new pages. The older pages
  // will be leaked on disk because no other pages reference them. This is a
  // defensive check because, in theory, we should never produce fewer pages
  // compared to what was originally in the chain.
  //
  // N.B. This can in fact happen with deletions or with updates to some key
  // using a larger value than the original. Our code might need more changes to
  // efficiently handle these scenaria.
  for (size_t i = new_num_pages; i < old_num_pages; ++i) {
    memset(chain->at(i)->GetData(), 0, Page::kSize);
    buf_mgr->UnfixPage(*(chain->at(i)), /*is_dirty=*/true);
  }
  if (new_num_pages < old_num_pages) {
    uint64_t lower = 0, upper = 0;
    lower =
        *reinterpret_cast<const uint64_t*>(std::get<0>(merged.front()).data());
    if (!std::get<0>(merged.back()).empty()) {
      upper =
          *reinterpret_cast<const uint64_t*>(std::get<0>(merged.back()).data());
    }
    lower = __builtin_bswap64(lower);
    upper = __builtin_bswap64(upper);
    Logger::Log(
        "WARNING: Reorganization produced fewer pages than the length of the "
        "original chain. Pages will be leaked on disk.\nOld Pages: %zu"
        ", New Pages: %zu\nChain Boundary Lower: %" PRIu64
        ", Chain Boundary Upper: %" PRIu64,
        old_num_pages, new_num_pages, lower, upper);
  }

  ++(stats->preorg_count_);
  stats->preorg_pre_total_length_ += old_num_pages;
  stats->preorg_post_total_length_ += new_num_pages;

  return Status::OK();
}

void ExtractOldRecords(OverflowChain chain, RecordBatch* old_records) {
  PageMergeIterator page_it(chain);

  while (page_it.Valid()) {
    old_records->emplace_back(page_it.key().ToString(),
                              page_it.value().ToString());
    page_it.Next();
  }
}

void MergeBatches(RecordBatch& old_records, const FlushBatch& records,
                  FlushBatch* merged) {
  // Setup.
  merged->reserve(old_records.size() + records.size());  // Overestimate.

  // Handle comparisons
  size_t i = 0, j = 0;
  while (i < old_records.size() && j < records.size()) {
    if (old_records[i].key() < std::get<0>(records[j])) {
      merged->emplace_back(old_records[i].key(), old_records[i].value(),
                           format::WriteType::kWrite);
      ++i;
    } else if (old_records[i].key() > std::get<0>(records[j])) {
      merged->emplace_back(records[j]);
      ++j;
    } else {
      if (std::get<2>(records[j]) == format::WriteType::kWrite)
        merged->emplace_back(records[j]);
      ++i;
      ++j;
    }
  }

  // Handle leftovers
  while (i < old_records.size()) {
    merged->emplace_back(old_records[i].key(), old_records[i].value(),
                         format::WriteType::kWrite);
    ++i;
  }

  while (j < records.size()) {
    merged->emplace_back(records[j]);
    ++j;
  }
}

}  // namespace tl
