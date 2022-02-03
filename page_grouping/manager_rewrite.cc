#include <deque>
#include <utility>
#include <vector>

#include "../bufmgr/page_memory_allocator.h"
#include "circular_page_buffer.h"
#include "manager.h"
#include "persist/merge_iterator.h"
#include "persist/page.h"
#include "persist/segment_wrap.h"
#include "util/key.h"

namespace {

using namespace llsm;
using namespace llsm::pg;

class PageChain {
 public:
  PageChain() : main_page_(nullptr), overflow_page_(nullptr) {}

  static PageChain SingleOnly(void* main) { return PageChain(main, nullptr); }
  static PageChain WithOverflow(void* main, void* overflow) {
    return PageChain(main, overflow);
  }

  pg::Page main() const { return pg::Page(main_page_); }
  std::optional<pg::Page> overflow() const {
    return overflow_page_ == nullptr ? std::optional<pg::Page>()
                                     : pg::Page(overflow_page_);
  }

  PageMergeIterator GetIterator() const {
    if (overflow_page_ != nullptr) {
      return PageMergeIterator(
          {main().GetIterator(), overflow()->GetIterator()});
    } else {
      return PageMergeIterator({main().GetIterator()});
    }
  }

 private:
  PageChain(void* main, void* overflow)
      : main_page_(main), overflow_page_(overflow) {}

  void* main_page_;
  void* overflow_page_;
};

}  // namespace

namespace llsm {
namespace pg {

void Manager::RewriteSegments(
    Key segment_base,
    const std::vector<std::pair<Key, Slice>>& additional_records,
    size_t record_start_idx, size_t record_end_idx, bool consider_neighbors) {
  // TODO: Multi-threading concerns.
  std::vector<std::pair<Key, SegmentInfo*>> segments_to_rewrite;
  const auto it = index_.lower_bound(segment_base);
  assert(it != index_.end());
  segments_to_rewrite.emplace_back(segment_base, &(it->second));

  // 1. Look up neighboring segments that can benefit from a rewrite.
  if (consider_neighbors) {
    // Scan backward.
    if (it != index_.begin()) {
      auto prev_it(it);
      while (true) {
        --prev_it;
        if (!prev_it->second.HasOverflow()) break;
        segments_to_rewrite.emplace_back(prev_it->first, &(prev_it->second));
        if (prev_it == index_.begin()) break;
      }
    }

    // Scan forward.
    auto next_it(it);
    ++next_it;
    for (; next_it != index_.end(); ++next_it) {
      if (!next_it->second.HasOverflow()) break;
      segments_to_rewrite.emplace_back(next_it->first, &(next_it->second));
    }

    // Sort the segments.
    std::sort(segments_to_rewrite.begin(), segments_to_rewrite.end(),
              [](const std::pair<Key, SegmentInfo*>& seg1,
                 const std::pair<Key, SegmentInfo*>& seg2) {
                return seg1.first < seg2.first;
              });
  }

  // 2. Load and merge the segments.
  //
  // General approach:
  // We use a logical "sliding window" over the segments to rewrite. We read in
  // segments, feed their records through a `SegmentBuilder`, and write out the
  // results into new segments on disk. We do this to (i) avoid reading all the
  // pages into memory at once and (ii) to do the rewrite in one pass.
  //
  // How large is the sliding window? We use a window of 4 * 16 = 64 pages.
  //
  // The largest segment we allow is 16 pages. Each segment can have up to one
  // overflow per page, meaning there are at most 16 overflows per segment.
  // Thus, assuming that the segments were built using the same goal and delta
  // parameters as the rewrite, we will span at most two 16 page segments in the
  // worst case.
  //
  // If this assumption does not hold, we may need to read in more data in the
  // worst case to fully "pack" a 16 page segment. If this happens and there is
  // no more memory available in our sliding window, we will just write
  // currently-being-built built segment onto disk instead.

  // Used for recovery.
  const uint32_t sequence_number = next_sequence_number_++;

  CircularPageBuffer page_buf(SegmentBuilder::kSegmentPageCounts.back() * 4);
  SegmentBuilder seg_builder(options_.records_per_page_goal,
                             options_.records_per_page_delta);

  // Keeps track of the pages in memory (the "sliding window"). The pages'
  // backing memory is in `page_buf`. The page chains in the deques are sorted
  // in ascending order (by key).
  //
  // `pages_to_process` contains page chains that need to be offered to the
  // `SegmentBuilder`. `pages_processed` contains chains that have been offered
  // to the `SegmentBuilder` but who still need to be kept around in memory
  // (because their records have not yet been written to new segments).
  std::deque<PageChain> pages_to_process, pages_processed;

  size_t next_record_idx = record_start_idx;

  for (const auto& seg_to_rewrite : segments_to_rewrite) {
    const size_t segment_pages = seg_to_rewrite.second->page_count();
    if (segment_pages > page_buf.NumFreePages()) {
      // TODO - finalize existing segment and clear pages.
    }

    // Load the segment and check for overflows.
    ReadSegment(seg_to_rewrite.second->id());
    SegmentWrap sw(w_.buffer().get(), seg_to_rewrite.second->page_count());
    const size_t num_overflows = sw.NumOverflows();
    if (segment_pages + num_overflows > page_buf.NumFreePages()) {
      // TODO - finalize existing segment and clear pages.
    }

    // Copy the segment pages into the buffer, leaving room for overflows as
    // needed.
    std::vector<PageChain> chains_in_segment;
    std::vector<std::pair<SegmentId, void*>> overflows_to_load;
    sw.ForEachPage([&](pg::Page page) {
      if (page.HasOverflow()) {
        void* main_page = page_buf.Allocate();
        void* overflow_page = page_buf.Allocate();
        memcpy(main_page, page.data().data(), page.data().size());
        chains_in_segment.push_back(
            PageChain::WithOverflow(main_page, overflow_page));
        overflows_to_load.emplace_back(page.GetOverflow(), overflow_page);

      } else {
        void* main_page = page_buf.Allocate();
        memcpy(main_page, page.data().data(), page.data().size());
        chains_in_segment.push_back(PageChain::SingleOnly(main_page));
      }
    });

    // Load all overflows into memory.
    ReadOverflows(overflows_to_load);

    // Add chains into the deque.
    pages_to_process.insert(pages_to_process.end(), chains_in_segment.begin(),
                            chains_in_segment.end());

    // Process the pages in the deque.
    while (!pages_to_process.empty()) {
      const PageChain& pc = pages_to_process.front();
      PageMergeIterator pmi = pc.GetIterator();

      while (pmi.Valid()) {
        // We merge the records on the page with additional records in memory.
        // The records in memory may duplicate the records on disk. If that
        // happens, they should get priority since they represent a rewrite.
        //
        // This code below implements this logic (essentially another iterator,
        // but inlined here).
        bool extract_from_pmi = true;
        bool skip_pmi_value = false;
        Key pmi_key = key_utils::ExtractHead64(pmi.key());
        // Check if the in-memory record should come first.
        if (next_record_idx < record_end_idx) {
          Key rec_key = additional_records[next_record_idx].first;
          extract_from_pmi = pmi_key < rec_key;
          // This happens if the in-memory record is an update of an existing
          // record stored on disk.
          skip_pmi_value = pmi_key == rec_key;
        }

        std::pair<Key, Slice> record =
            extract_from_pmi ? std::pair<Key, Slice>(pmi_key, pmi.value())
                             : additional_records[next_record_idx];

        auto segments = seg_builder.Offer(std::move(record));
        if (segments.size() > 0) {
          // TODO - Write segments to disk, remove pages from `pages_processed`,
          // free pages from the page buffer.
        }

        if (extract_from_pmi) {
          pmi.Next();
        } else {
          ++next_record_idx;
          if (skip_pmi_value) {
            pmi.Next();
          }
        }
      }

      // Done with this page chain.
      pages_processed.push_back(pages_to_process.front());
      pages_to_process.pop_front();
    }
  }
}

}  // namespace pg
}  // namespace llsm
