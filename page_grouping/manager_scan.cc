#include "manager.h"

#include <vector>
#include <utility>

#include "persist/merge_iterator.h"
#include "util/key.h"

namespace llsm {
namespace pg {

Status Manager::ScanWithEstimates(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  // Scan strategy (all scans are forward scans):
  // - Find segment containing starting key.
  // - Estimate how much of the segment to read based on the position of the
  // key.
  // - Scan forward, reading the whole segment when able.
  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  auto it = index_.upper_bound(start_key);
  if (it != index_.begin()) {
    --it;
  }

  // 2. Estimate how much of the segment to read based on the position of the
  // key.
  size_t est_start_pages_to_read = 1;
  size_t start_page_idx = 0;
  const size_t first_segment_page_count = it->second.page_count();
  if (first_segment_page_count > 1) {
    const Key base_key = it->first;
    const double pos =
        std::max(0.0, it->second.model()->operator()(start_key - base_key));
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
  // our estimate was incorrect.
  const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
  const size_t segment_byte_offset = it->second.id().GetOffset() * Page::kSize;
  sf.ReadPages(segment_byte_offset + start_page_idx * Page::kSize,
               w_.buffer().get(), est_start_pages_to_read);
  w_.BumpReadCount(est_start_pages_to_read);

  // The workspace buffer has one extra page at the end for use as the overflow.
  void* overflow_buf =
      w_.buffer().get() +
      (SegmentBuilder::kSegmentPageCounts.back()) * pg::Page::kSize;
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
    ++start_seg_page_idx;
  }

  // If we estimated incorrectly and we still have more pages to read in the
  // first segment.
  while (records_left > 0 && start_seg_page_idx < first_segment_page_count) {
    // Read 1 page at a time.
    sf.ReadPages(segment_byte_offset + start_seg_page_idx * Page::kSize,
                 w_.buffer().get(), /*num_pages=*/1);
    w_.BumpReadCount(1);
    Page page(w_.buffer().get());
    scan_page(page);
    ++start_seg_page_idx;
  }

  // 4. Done reading the first segment. Now keep scanning forward as far as
  // needed.
  if (it != index_.end()) {
    ++it;
  }
  while (records_left > 0 && it != index_.end()) {
    const size_t seg_page_count = it->second.page_count();
    const size_t seg_byte_offset = it->second.id().GetOffset() * Page::kSize;
    const size_t est_pages_left = std::ceil(
        records_left / static_cast<double>(options_.records_per_page_goal));

    // Estimate the number of pages to read from the segment. The idea is to
    // avoid reading the whole segment if we do not anticipate needing to scan
    // all the records in the segment.
    const size_t pages_to_read = std::min(seg_page_count, est_pages_left);
    const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
    sf.ReadPages(seg_byte_offset, w_.buffer().get(), pages_to_read);
    w_.BumpReadCount(pages_to_read);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < pages_to_read) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      ++page_idx;
    }

    // If we underestimated and need to read a few more pages from this
    // segment.
    while (records_left > 0 && page_idx < seg_page_count) {
      // Read 1 page at a time.
      sf.ReadPages(seg_byte_offset + page_idx * Page::kSize, w_.buffer().get(),
                   /*num_pages=*/1);
      w_.BumpReadCount(1);
      Page page(w_.buffer().get());
      scan_page(page);
      ++page_idx;
    }

    // Go to the next segment.
    ++it;
  }

  return Status::OK();
}

Status Manager::ScanWhole(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  auto it = index_.upper_bound(start_key);
  if (it != index_.begin()) {
    --it;
  }

  // 2. Read the first segment.
  const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
  const size_t first_segment_size = it->second.page_count();
  const size_t segment_byte_offset = it->second.id().GetOffset() * Page::kSize;
  sf.ReadPages(segment_byte_offset, w_.buffer().get(), first_segment_size);
  w_.BumpReadCount(first_segment_size);

  // The workspace buffer has one extra page at the end for use as the overflow.
  void* overflow_buf =
      w_.buffer().get() +
      (SegmentBuilder::kSegmentPageCounts.back()) * pg::Page::kSize;
  Page overflow_page(overflow_buf);

  // 3. Scan the first matching page in the segment.
  size_t start_segment_page_idx = it->second.PageForKey(it->first, start_key);
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
    ++start_segment_page_idx;
  }

  // 5. Scan forward until we read enough records or run out of segments to
  //    read.
  if (records_left > 0 && it != index_.end()) {
    ++it;
  }
  while (records_left > 0 && it != index_.end()) {
    const size_t seg_page_count = it->second.page_count();
    const size_t seg_byte_offset = it->second.id().GetOffset() * Page::kSize;

    const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
    sf.ReadPages(seg_byte_offset, w_.buffer().get(), seg_page_count);
    w_.BumpReadCount(seg_page_count);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < seg_page_count) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      ++page_idx;
    }

    // Go to the next segment.
    ++it;
  }

  return Status::OK();
}

}  // namespace pg
}  // namespace llsm
