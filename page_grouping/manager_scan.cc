#include <vector>

#include "manager.h"
#include "persist/merge_iterator.h"
#include "util/key.h"

namespace tl {
namespace pg {

using SegmentMode = LockManager::SegmentMode;
using PageMode = LockManager::PageMode;

Status Manager::ScanWithEstimates(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  // Scan strategy (all scans are forward scans):
  // - Find segment containing starting key.
  // - Estimate how much of the segment to read based on the position of the
  // key.
  // - Scan forward, reading the whole segment when able.
  //
  // Locking strategy:
  // - Segment locks are acquired in `kPageRead` mode using lock coupling
  // (acquire the next segment lock before releasing the previous one).
  // - Segment locks are acquired in logically ascending order (by key).
  // - Page locks are acquired in shared mode. A page's lock can be released as
  // soon as it has been scanned.
  // - Page locks are also acquired in ascending order.
  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  const auto start_seg =
      index_->SegmentForKeyWithLock(start_key, SegmentMode::kPageRead);

  // 2. Estimate how much of the segment to read based on the position of the
  // key.
  size_t est_start_pages_to_read = 1;
  size_t start_page_idx = 0;
  const size_t first_segment_page_count = start_seg.sinfo.page_count();
  if (first_segment_page_count > 1) {
    const double pos = std::max(
        0.0, start_seg.sinfo.model()->operator()(start_key - start_seg.lower));
    const size_t page_idx = static_cast<size_t>(pos);
    if (page_idx >= first_segment_page_count) {
      // Edge case due to numeric errors.
      start_page_idx = first_segment_page_count - 1;
      est_start_pages_to_read = 1;
    } else {
      // Use the predicted position to estimate how many more pages we need to
      // read.
      const double page_pos = pos - page_idx;
      const int64_t est_matching_records_on_first_page =
          options_.records_per_page_goal -
          (page_pos * options_.records_per_page_goal);
      const int64_t est_remaining_records =
          records_left - est_matching_records_on_first_page;

      start_page_idx = page_idx;
      if (est_remaining_records > 0) {
        const size_t est_remaining_pages =
            std::ceil(est_remaining_records /
                      static_cast<double>(options_.records_per_page_goal));
        est_start_pages_to_read =
            1 + std::min(est_remaining_pages,
                         first_segment_page_count - page_idx - 1);
      } else {
        est_start_pages_to_read = 1;
      }
    }
  }

  // 3. Start scanning the first segment, reading in more pages as needed if
  // our estimate was incorrect. We acquire shared page locks before doing the
  // actual read.
  for (size_t page_idx = start_page_idx;
       page_idx < start_page_idx + est_start_pages_to_read; ++page_idx) {
    lock_manager_->AcquirePageLock(start_seg.sinfo.id(), page_idx,
                                   PageMode::kShared);
  }
  const std::unique_ptr<SegmentFile>& sf =
      segment_files_[start_seg.sinfo.id().GetFileId()];
  const size_t segment_byte_offset =
      start_seg.sinfo.id().GetOffset() * Page::kSize;
  sf->ReadPages(segment_byte_offset + start_page_idx * Page::kSize,
                w_.buffer().get(), est_start_pages_to_read);
  w_.BumpReadCount(est_start_pages_to_read);

  // The workspace buffer has one extra page at the end for use as the overflow.
  void* overflow_buf =
      w_.buffer().get() +
      (SegmentBuilder::SegmentPageCounts().back()) * pg::Page::kSize;
  Page overflow_page(overflow_buf);

  // Scan the first page.
  Page first_page(w_.buffer().get());
  std::vector<Page::Iterator> page_its = {first_page.GetIterator()};
  if (first_page.HasOverflow()) {
    ReadPage(first_page.GetOverflow(), 0, overflow_buf);
    page_its.push_back(overflow_page.GetIterator());
  }
  key_utils::IntKeyAsSlice start_key_slice_helper(start_key);
  Slice start_key_slice = start_key_slice_helper.as<Slice>();
  PageMergeIterator pmi(std::move(page_its), &start_key_slice);
  for (; records_left > 0 && pmi.Valid(); --records_left, pmi.Next()) {
    values_out->emplace_back(key_utils::ExtractHead64(pmi.key()),
                             pmi.value().ToString());
  }
  lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_page_idx,
                                 PageMode::kShared);

  // Common code used to scan a whole page.
  const auto scan_page = [this, &records_left, &overflow_page, overflow_buf,
                          values_out](const Page& page) {
    std::vector<Page::Iterator> page_its = {page.GetIterator()};
    if (page.HasOverflow()) {
      ReadPage(page.GetOverflow(), 0, overflow_buf);
      page_its.push_back(overflow_page.GetIterator());
    }

    PageMergeIterator pmi(std::move(page_its));
    for (; records_left > 0 && pmi.Valid(); --records_left, pmi.Next()) {
      values_out->emplace_back(key_utils::ExtractHead64(pmi.key()),
                               pmi.value().ToString());
    }
  };

  // Scan the rest of the pages in the segment that we read in.
  size_t start_seg_page_idx = start_page_idx + 1;
  while (records_left > 0 &&
         start_seg_page_idx < (start_page_idx + est_start_pages_to_read)) {
    Page page(w_.buffer().get() +
              (start_seg_page_idx - start_page_idx) * Page::kSize);
    scan_page(page);
    lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_seg_page_idx,
                                   PageMode::kShared);
    ++start_seg_page_idx;
  }
  // Release any page locks that were not released by the loop above (this will
  // happen if `records_left == 0` before scanning all the read-in pages).
  for (; start_seg_page_idx < (start_page_idx + est_start_pages_to_read);
       ++start_seg_page_idx) {
    lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_seg_page_idx,
                                   PageMode::kShared);
  }

  // If we estimated incorrectly and we still have more pages to read in the
  // first segment.
  while (records_left > 0 && start_seg_page_idx < first_segment_page_count) {
    // Read 1 page at a time.
    lock_manager_->AcquirePageLock(start_seg.sinfo.id(), start_seg_page_idx,
                                   PageMode::kShared);
    sf->ReadPages(segment_byte_offset + start_seg_page_idx * Page::kSize,
                  w_.buffer().get(), /*num_pages=*/1);
    w_.BumpReadCount(1);
    Page page(w_.buffer().get());
    scan_page(page);
    lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_seg_page_idx,
                                   PageMode::kShared);
    ++start_seg_page_idx;
  }

  if (records_left == 0) {
    // No need to scan the next segment.
    lock_manager_->ReleaseSegmentLock(start_seg.sinfo.id(),
                                      SegmentMode::kPageRead);
    return Status::OK();
  }

  // 4. Now keep scanning forward as far as needed.
  SegmentId prev_seg_id = start_seg.sinfo.id();
  std::optional<SegmentIndex::Entry> curr_seg =
      index_->NextSegmentForKeyWithLock(start_seg.lower,
                                        SegmentMode::kPageRead);
  while (records_left > 0 && curr_seg.has_value()) {
    lock_manager_->ReleaseSegmentLock(prev_seg_id, SegmentMode::kPageRead);

    const size_t seg_page_count = curr_seg->sinfo.page_count();
    const size_t seg_byte_offset =
        curr_seg->sinfo.id().GetOffset() * Page::kSize;
    const size_t est_pages_left = std::ceil(
        records_left / static_cast<double>(options_.records_per_page_goal));

    // Estimate the number of pages to read from the segment. The idea is to
    // avoid reading the whole segment if we do not anticipate needing to scan
    // all the records in the segment.
    const size_t pages_to_read = std::min(seg_page_count, est_pages_left);
    for (size_t page_idx = 0; page_idx < pages_to_read; ++page_idx) {
      lock_manager_->AcquirePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
    }
    const std::unique_ptr<SegmentFile>& sf =
        segment_files_[curr_seg->sinfo.id().GetFileId()];
    sf->ReadPages(seg_byte_offset, w_.buffer().get(), pages_to_read);
    w_.BumpReadCount(pages_to_read);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < pages_to_read) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      lock_manager_->ReleasePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
      ++page_idx;
    }
    // Release any page locks that were not released by the loop above.
    for (; page_idx < pages_to_read; ++page_idx) {
      lock_manager_->ReleasePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
    }

    // If we underestimated and need to read a few more pages from this
    // segment.
    while (records_left > 0 && page_idx < seg_page_count) {
      lock_manager_->AcquirePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
      // Read 1 page at a time.
      sf->ReadPages(seg_byte_offset + page_idx * Page::kSize, w_.buffer().get(),
                    /*num_pages=*/1);
      w_.BumpReadCount(1);
      Page page(w_.buffer().get());
      scan_page(page);
      lock_manager_->ReleasePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
      ++page_idx;
    }

    // Go to the next segment. To avoid an unnecessary lock acquisiton, we check
    // first if we need more records.
    prev_seg_id = curr_seg->sinfo.id();
    if (records_left > 0) {
      curr_seg = index_->NextSegmentForKeyWithLock(curr_seg->lower,
                                                   SegmentMode::kPageRead);
    }
  }

  lock_manager_->ReleaseSegmentLock(prev_seg_id, SegmentMode::kPageRead);
  return Status::OK();
}

Status Manager::ScanWhole(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  values_out->clear();
  values_out->reserve(amount);

  // Locking strategy:
  // - Segment locks are acquired in `kPageRead` mode using lock coupling
  // (acquire the next segment lock before releasing the previous one).
  // - Segment locks are acquired in logically ascending order (by key).
  // - Page locks are acquired in shared mode. A page's lock can be released as
  // soon as it has been scanned.
  // - Page locks are also acquired in ascending order.

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  const auto start_seg =
      index_->SegmentForKeyWithLock(start_key, SegmentMode::kPageRead);

  // 2. Read the first segment.
  const std::unique_ptr<SegmentFile>& sf =
      segment_files_[start_seg.sinfo.id().GetFileId()];
  const size_t first_segment_size = start_seg.sinfo.page_count();
  const size_t segment_byte_offset =
      start_seg.sinfo.id().GetOffset() * Page::kSize;
  size_t start_segment_page_idx =
      start_seg.sinfo.PageForKey(start_seg.lower, start_key);
  // We read the whole segment but start scanning from `start_segment_page_idx`.
  // So we only need page locks starting from this index.
  for (size_t page_idx = start_segment_page_idx; page_idx < first_segment_size;
       ++page_idx) {
    lock_manager_->AcquirePageLock(start_seg.sinfo.id(), page_idx,
                                   PageMode::kShared);
  }
  sf->ReadPages(segment_byte_offset, w_.buffer().get(), first_segment_size);
  w_.BumpReadCount(first_segment_size);

  // The workspace buffer has one extra page at the end for use as the overflow.
  void* overflow_buf =
      w_.buffer().get() +
      (SegmentBuilder::SegmentPageCounts().back()) * pg::Page::kSize;
  Page overflow_page(overflow_buf);

  // 3. Scan the first matching page in the segment.
  Page first_page(w_.buffer().get() + start_segment_page_idx * Page::kSize);
  std::vector<Page::Iterator> page_its = {first_page.GetIterator()};
  if (first_page.HasOverflow()) {
    ReadPage(first_page.GetOverflow(), 0, overflow_buf);
    page_its.push_back(overflow_page.GetIterator());
  }
  key_utils::IntKeyAsSlice start_key_slice_helper(start_key);
  Slice start_key_slice = start_key_slice_helper.as<Slice>();
  PageMergeIterator pmi(std::move(page_its), &start_key_slice);
  for (; pmi.Valid() && records_left > 0; pmi.Next(), --records_left) {
    values_out->emplace_back(key_utils::ExtractHead64(pmi.key()),
                             pmi.value().ToString());
  }
  lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_segment_page_idx,
                                 PageMode::kShared);

  // Common code used to scan a whole page.
  const auto scan_page = [this, &records_left, &overflow_page, overflow_buf,
                          values_out](const Page& page) {
    std::vector<Page::Iterator> page_its = {page.GetIterator()};
    if (page.HasOverflow()) {
      ReadPage(page.GetOverflow(), 0, overflow_buf);
      page_its.push_back(overflow_page.GetIterator());
    }

    PageMergeIterator pmi(std::move(page_its));
    for (; records_left > 0 && pmi.Valid(); --records_left, pmi.Next()) {
      values_out->emplace_back(key_utils::ExtractHead64(pmi.key()),
                               pmi.value().ToString());
    }
  };

  // 4. Scan the rest of the pages in the segment.
  ++start_segment_page_idx;
  while (records_left > 0 && start_segment_page_idx < first_segment_size) {
    Page page(w_.buffer().get() + start_segment_page_idx * Page::kSize);
    scan_page(page);
    lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_segment_page_idx,
                                   PageMode::kShared);
    ++start_segment_page_idx;
  }
  // Release any page locks that were not released by the loop above (this will
  // happen if `records_left == 0` before scanning all the read-in pages).
  for (; start_segment_page_idx < first_segment_size;
       ++start_segment_page_idx) {
    lock_manager_->ReleasePageLock(start_seg.sinfo.id(), start_segment_page_idx,
                                   PageMode::kShared);
  }

  // No more records needed. Return early.
  if (records_left == 0) {
    lock_manager_->ReleaseSegmentLock(start_seg.sinfo.id(),
                                      SegmentMode::kPageRead);
    return Status::OK();
  }

  // 5. Scan forward until we read enough records or run out of segments to
  //    read.
  SegmentId prev_seg_id = start_seg.sinfo.id();
  std::optional<SegmentIndex::Entry> curr_seg =
      index_->NextSegmentForKeyWithLock(start_seg.lower,
                                        SegmentMode::kPageRead);
  while (records_left > 0 && curr_seg.has_value()) {
    lock_manager_->ReleaseSegmentLock(prev_seg_id, SegmentMode::kPageRead);

    const size_t seg_page_count = curr_seg->sinfo.page_count();
    const size_t seg_byte_offset =
        curr_seg->sinfo.id().GetOffset() * Page::kSize;

    for (size_t page_idx = 0; page_idx < seg_page_count; ++page_idx) {
      lock_manager_->AcquirePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
    }
    const std::unique_ptr<SegmentFile>& sf =
        segment_files_[curr_seg->sinfo.id().GetFileId()];
    sf->ReadPages(seg_byte_offset, w_.buffer().get(), seg_page_count);
    w_.BumpReadCount(seg_page_count);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < seg_page_count) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      lock_manager_->ReleasePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
      ++page_idx;
    }
    // Release any page locks that were not released by the loop above.
    for (; page_idx < seg_page_count; ++page_idx) {
      lock_manager_->ReleasePageLock(curr_seg->sinfo.id(), page_idx,
                                     PageMode::kShared);
    }

    // Go to the next segment.
    prev_seg_id = curr_seg->sinfo.id();
    if (records_left > 0) {
      curr_seg = index_->NextSegmentForKeyWithLock(curr_seg->lower,
                                                   SegmentMode::kPageRead);
    }
  }

  lock_manager_->ReleaseSegmentLock(prev_seg_id, SegmentMode::kPageRead);
  return Status::OK();
}

}  // namespace pg
}  // namespace tl
