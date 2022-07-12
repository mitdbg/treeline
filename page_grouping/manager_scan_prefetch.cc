#include <future>
#include <new>
#include <sstream>
#include <utility>

#include "manager.h"
#include "persist/merge_iterator.h"
#include "treeline/pg_stats.h"
#include "util/key.h"
#include "workspace.h"

namespace {

class PrefetchBuffer {
 public:
  PrefetchBuffer(char* buf, size_t pages) : buf_(buf), pages_left_(pages) {}

  char* Allocate(size_t pages) {
    if (pages > pages_left_) {
      // We do not expect to hit this case in our experiments. So we want this
      // case to be a fatal error.
      throw std::bad_alloc();
    }
    char* const to_return = buf_;
    buf_ += tl::pg::Page::kSize * pages;
    pages_left_ -= pages;
    return to_return;
  }

 private:
  char* buf_;
  size_t pages_left_;
};

}  // namespace

namespace tl {
namespace pg {

using SegmentMode = LockManager::SegmentMode;
using PageMode = LockManager::PageMode;

Status Manager::ScanWithExperimentalPrefetching(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  // Scan strategy (all scans are forward scans):
  // - Estimate how many pages we will need to read.
  // - Make read requests for as many segments as needed (prefetching).
  // - Start scanning the segments as they are fetched, loading overflows as
  // needed.
  //
  // NOTE: For implementation simplicity, no locks are held. This implementation
  // is **experimental** and not designed to run concurrently with writes.

  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // This is a rough estimate that should be good enough for the prefetching
  // experiments.
  const size_t est_pages_to_fetch =
      2ULL + std::ceil(records_left /
                       static_cast<double>(options_.records_per_page_goal));
  size_t pages_prefetched = 0;

  // 1. Find the segment that should hold the start key.
  const auto start_seg =
      index_->SegmentForKeyWithLock(start_key, SegmentMode::kPageRead);
  lock_manager_->ReleaseSegmentLock(start_seg.sinfo.id(),
                                    SegmentMode::kPageRead);

  // 2. Compute how much of the segment to read based on the position of the
  // key.
  size_t start_pages_to_read = 1;
  size_t start_page_idx = 0;
  const size_t first_segment_page_count = start_seg.sinfo.page_count();
  if (first_segment_page_count > 1) {
    const double pos = std::max(
        0.0, start_seg.sinfo.model()->operator()(start_key - start_seg.lower));
    const size_t page_idx = static_cast<size_t>(pos);
    if (page_idx >= first_segment_page_count) {
      // Edge case due to numeric errors.
      start_page_idx = first_segment_page_count - 1;
      start_pages_to_read = 1;
    } else {
      // Always read the rest of the segment.
      start_page_idx = page_idx;
      start_pages_to_read = first_segment_page_count - start_page_idx;
    }
  }

  // Used to handle prefetching.
  std::vector<std::future<std::pair<char*, size_t>>> ready_pages;
  PrefetchBuffer prefetch_buf(w_.prefetch_buffer().get(),
                              Workspace::kPrefetchBufferPages);

  // 3. Fetch the first segment.
  ready_pages.emplace_back(
      bg_threads_->Submit([this, start_seg, start_page_idx, start_pages_to_read,
                           buf = prefetch_buf.Allocate(start_pages_to_read)]() {
        const std::unique_ptr<SegmentFile>& sf =
            segment_files_[start_seg.sinfo.id().GetFileId()];
        const size_t segment_byte_offset =
            start_seg.sinfo.id().GetOffset() * Page::kSize;
        sf->ReadPages(segment_byte_offset + start_page_idx * Page::kSize, buf,
                      start_pages_to_read);
        w_.BumpReadCount(start_pages_to_read);
        return std::make_pair(buf, start_pages_to_read);
      }));
  pages_prefetched += start_pages_to_read;

  // 4. Fetch additional segments until we exhaust our estimate.
  SegmentId prev_seg_id = start_seg.sinfo.id();
  std::optional<SegmentIndex::Entry> curr_seg =
      index_->NextSegmentForKeyWithLock(start_seg.lower,
                                        SegmentMode::kPageRead);
  if (curr_seg.has_value()) {
    lock_manager_->ReleaseSegmentLock(curr_seg->sinfo.id(),
                                      SegmentMode::kPageRead);
  }

  // Flag used for correctness checking.
  bool has_pages_remaining_in_last_segment = false;

  while (pages_prefetched < est_pages_to_fetch && curr_seg.has_value()) {
    const size_t seg_page_count = curr_seg->sinfo.page_count();
    const size_t seg_byte_offset =
        curr_seg->sinfo.id().GetOffset() * Page::kSize;
    const size_t pages_to_read =
        std::min(seg_page_count, est_pages_to_fetch - pages_prefetched);
    if (pages_to_read < seg_page_count) {
      has_pages_remaining_in_last_segment = true;
    }

    ready_pages.emplace_back(bg_threads_->Submit(
        [this, curr_seg = *curr_seg, seg_byte_offset, pages_to_read,
         buf = prefetch_buf.Allocate(pages_to_read)]() {
          const std::unique_ptr<SegmentFile>& sf =
              segment_files_[curr_seg.sinfo.id().GetFileId()];
          sf->ReadPages(seg_byte_offset, buf, pages_to_read);
          w_.BumpReadCount(pages_to_read);
          return std::make_pair(buf, pages_to_read);
        }));
    pages_prefetched += seg_page_count;

    // Go to the next segment. To avoid an unnecessary lock acquisiton, we check
    // first if we need more records.
    prev_seg_id = curr_seg->sinfo.id();
    if (pages_prefetched < est_pages_to_fetch) {
      curr_seg = index_->NextSegmentForKeyWithLock(curr_seg->lower,
                                                   SegmentMode::kPageRead);
      if (curr_seg.has_value()) {
        lock_manager_->ReleaseSegmentLock(curr_seg->sinfo.id(),
                                          SegmentMode::kPageRead);
      }
    }
  }

  // 5. Scan through the prefetched pages. For correctness, we need to load
  // overflows if they exist.

  void* overflow_buf = w_.buffer().get();
  Page overflow_page(overflow_buf);

  // Code used to scan the first page (requires a lower bound seek).
  const auto scan_first_page = [this, &records_left, &overflow_page,
                                overflow_buf, start_key,
                                values_out](const Page& first_page) {
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
  };

  // Common code used to scan a whole page.
  const auto scan_page = [this, &records_left, &overflow_page, overflow_buf,
                          values_out](const Page& page) {
    std::vector<Page::Iterator> page_its = {page.GetIterator()};
    if (page.HasOverflow()) {
      ReadPage(page.GetOverflow(), 0, overflow_buf);
      page_its.push_back(overflow_page.GetIterator());
    }

    PageMergeIterator pmi(std::move(page_its));
    if (pmi.Valid()) {
      assert(pmi.RecordsLeft() > 0);
    }
    for (; records_left > 0 && pmi.Valid(); --records_left, pmi.Next()) {
      assert(pmi.RecordsLeft() > 0);
      values_out->emplace_back(key_utils::ExtractHead64(pmi.key()),
                               pmi.value().ToString());
    }
  };

  bool is_first_page = true;
  size_t fetched_pages_used = 0;
  for (auto& f : ready_pages) {
    if (records_left == 0) break;

    // Wait for the I/O to complete.
    const auto [buf, num_pages] = f.get();

    for (size_t i = 0; i < num_pages && records_left > 0; ++i) {
      Page page(buf + Page::kSize * i);
      if (is_first_page) {
        is_first_page = false;
        scan_first_page(page);
      } else {
        scan_page(page);
      }
      ++fetched_pages_used;
    }
  }

  if (records_left > 0 &&
      (curr_seg.has_value() || has_pages_remaining_in_last_segment)) {
    // This represents a situation where we underestimated the number of pages
    // to prefetch. We want this case to be a fatal error to avoid incorrectness
    // in the experiments.
    std::stringstream err_msg;
    err_msg << "Underestimated prefetch. Scan length: " << amount
            << " Estimated pages: " << est_pages_to_fetch
            << " Scanned records: " << (amount - records_left);
    throw std::runtime_error(err_msg.str());
  }

  // If we reach here, it must be the case that `est_pages_to_fetch >=
  // fetched_pages_used`. We track the number of "overfetched" pages.
  // N.B. This thread will implicitly wait for any remaining outstanding I/Os.
  PageGroupedDBStats::Local().BumpOverfetchedPages(est_pages_to_fetch -
                                                   fetched_pages_used);

  return Status::OK();
}

}  // namespace pg
}  // namespace tl
