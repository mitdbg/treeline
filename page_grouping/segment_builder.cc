#include "segment_builder.h"

#include <algorithm>

#include "plr/data.h"
#include "plr/greedy.h"

namespace {

using Record = std::pair<llsm::pg::Key, const llsm::Slice>;

// The number of pages in each segment.
static const std::vector<size_t> kSegmentPageCounts = {1, 2, 4, 8, 16};

// The maximum number of pages in any segment.
static const size_t kMaxSegmentSize = kSegmentPageCounts.back();

}  // namespace

namespace llsm {
namespace pg {

SegmentBuilder::SegmentBuilder(const size_t records_per_page_goal,
                               const size_t records_per_page_delta)
    : records_per_page_goal_(records_per_page_goal),
      records_per_page_delta_(records_per_page_delta),
      max_records_in_segment_(records_per_page_goal_ * kMaxSegmentSize) {
  allowed_records_per_segment_.reserve(kSegmentPageCounts.size());
  for (size_t pages : kSegmentPageCounts) {
    allowed_records_per_segment_.push_back(pages * max_records_in_segment_);
  }
}

std::vector<Segment> SegmentBuilder::Build(
    const std::vector<std::pair<Key, const Slice>>& dataset) {
  // Precondition: The dataset is sorted by key in ascending order.
  size_t next_idx = 0;
  std::vector<size_t> records_processed;
  std::vector<Segment> segments;
  records_processed.reserve(allowed_records_per_segment_.back());

  while (next_idx < dataset.size()) {
    records_processed.clear();

    const Record& base = dataset[next_idx];
    const Key base_key = base.first;
    records_processed.push_back(next_idx);
    next_idx++;

    auto plr = plr::GreedyPLRBuilder64(records_per_page_delta_);
    auto maybe_line = plr.Offer(plr::Point64(0, 0));
    // Only one point.
    assert(!maybe_line.has_value());

    // Attempt to build a model covering as much data as possible.
    while (next_idx < dataset.size() &&
           records_processed.size() < max_records_in_segment_) {
      const Record& next = dataset[next_idx];
      const Key diff = next.first - base_key;
      // This algorithm assumes equally sized keys. So we give each record
      // equal weight; a straightfoward approach is just to count up by 1.
      maybe_line = plr.Offer(plr::Point64(diff, records_processed.size()));
      if (maybe_line.has_value()) {
        // We cannot extend the model further to include this record.
        break;
      }
      records_processed.push_back(next_idx);
      next_idx++;
    }

    if (!maybe_line.has_value()) {
      // We exited the loop because we exceeded the number of records per
      // segment, or ran out of records in the dataset.
      maybe_line = plr.Finish();
      if (!maybe_line.has_value()) {
        // This should only happen when there is a single record left.
        assert(records_processed.size() == 1);
        // TODO: Allocate the segment here.
        continue;
      }
    }

    // Determine how large of a segment we can make.
    const auto upper_bound_it = std::upper_bound(
        allowed_records_per_segment_.begin(),
        allowed_records_per_segment_.end(), records_processed.size());
    const int64_t segment_size_idx =
        static_cast<int>(upper_bound_it -
                         allowed_records_per_segment_.begin()) -
        1;

    if (segment_size_idx < 0) {
      // Could not build a model that "covers" one full page. So we just fill a
      // page anyways.
      const size_t addtl_keys =
          allowed_records_per_segment_[0] - records_processed.size();
      // TODO: Allocate the segment here.
      next_idx += addtl_keys;
      continue;
    }

    const size_t segment_size = kSegmentPageCounts[segment_size_idx];
    if (segment_size == 1) {
      // No need for a model.
      const size_t extra_keys =
          records_processed.size() - allowed_records_per_segment_[0];
      // TODO: Allocate the segment here.
      next_idx -= extra_keys;
      continue;
    }

    const size_t records_in_segment =
        allowed_records_per_segment_[segment_size_idx];

    // Use the model to determine how many records to place in the segment,
    // based on the desired goal. We use binary search against the model to
    // minimize the effect of precision errors.
    const auto cutoff_it = std::lower_bound(
        records_processed.begin(), records_processed.end(), records_in_segment,
        [&dataset, &maybe_line](const size_t rec_idx_a, const size_t rec_idx_b) {
          const Key key_a = dataset[rec_idx_a];
          const Key key_b = dataset[rec_idx_b];
          const auto pos_a = maybe_line->line(key_a - base_key);
          const auto pos_b = maybe_line->line(key_b - base_key);
          return pos_a < pos_b;
        });
    assert(cutoff_it != records_processed.begin());
    const size_t extra_keys = records_processed.end() - cutoff_it;
    next_idx -= extra_keys;
    // TODO: Allocate the segment here.
  }

  return segments;
}

}  // namespace pg
}  // namespace llsm
