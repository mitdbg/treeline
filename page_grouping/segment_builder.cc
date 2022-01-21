#include "segment_builder.h"

#include <algorithm>

#include "plr/data.h"
#include "plr/greedy.h"

namespace llsm {
namespace pg {

using Record = std::pair<llsm::pg::Key, const llsm::Slice>;

// The number of pages in each segment. If you change this, change
// `kPageCountToSegment` too.
const std::vector<size_t> SegmentBuilder::kSegmentPageCounts = {1, 2, 4, 8, 16};

const std::unordered_map<size_t, size_t> SegmentBuilder::kPageCountToSegment = {
    {1, 0}, {2, 1}, {4, 2}, {8, 3}, {16, 4}};

// The maximum number of pages in any segment.
static const size_t kMaxSegmentSize =
    llsm::pg::SegmentBuilder::kSegmentPageCounts.back();

Segment Segment::MultiPage(size_t page_count, size_t start_idx, size_t end_idx,
                           plr::BoundedLine64 model) {
  Segment s;
  s.page_count = page_count;
  s.start_idx = start_idx;
  s.end_idx = end_idx;
  s.model = model;
  return s;
}

Segment Segment::SinglePage(size_t start_idx, size_t end_idx) {
  Segment s;
  s.page_count = 1;
  s.start_idx = start_idx;
  s.end_idx = end_idx;
  s.model = std::optional<plr::BoundedLine64>();
  return s;
}

SegmentBuilder::SegmentBuilder(const size_t records_per_page_goal,
                               const size_t records_per_page_delta)
    : records_per_page_goal_(records_per_page_goal),
      records_per_page_delta_(records_per_page_delta),
      max_records_in_segment_(
          (records_per_page_goal_ + records_per_page_delta_) *
          kMaxSegmentSize) {
  allowed_records_per_segment_.reserve(kSegmentPageCounts.size());
  for (size_t pages : kSegmentPageCounts) {
    allowed_records_per_segment_.push_back(pages * records_per_page_goal_);
  }
}

std::vector<Segment> SegmentBuilder::Build(
    const std::vector<std::pair<Key, Slice>>& dataset) {
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
        segments.push_back(Segment::SinglePage(records_processed.front(),
                                               records_processed.back() + 1));
        continue;
      }
    }

    // According to the generated model, how many records have we processed when
    // we hit the last key?
    const double last_record_size =
        1.0 +
        std::max(0.0, maybe_line->line()(
                          dataset[records_processed.back()].first - base_key));

    // Find the largest possible segment we can make.
    // `allowed_records_per_segment_` contains the ideal size for each segment.
    // We want to find the index of the *largest* value that is less than or equal
    // to `last_record_size`.
    const auto segment_size_idx_it =
        std::upper_bound(allowed_records_per_segment_.begin(),
                         allowed_records_per_segment_.end(), last_record_size);
    const int segment_size_idx =
        (segment_size_idx_it - allowed_records_per_segment_.begin()) - 1;

    if (segment_size_idx <= 0) {
      // One of two cases:
      // - Could not build a model that "covers" one full page
      // - Could not build a model that "covers" more than one page
      // So we just fill one page regardless and omit the model.
      const size_t target_size = allowed_records_per_segment_[0];
      if (target_size > records_processed.size()) {
        // Can add more records.
        while (records_processed.size() < target_size &&
               next_idx < dataset.size()) {
          records_processed.push_back(next_idx++);
        }
      } else if (target_size < records_processed.size()) {
        // Have too many records.
        size_t extra_keys = records_processed.size() - target_size;
        while (extra_keys > 0) {
          --next_idx;
          --extra_keys;
        }
      }
      segments.push_back(Segment::SinglePage(records_processed.front(),
                                             records_processed.back() + 1));
      continue;
    }

    const size_t segment_size = kSegmentPageCounts[segment_size_idx];
    assert(segment_size > 1);

    const size_t records_in_segment =
        allowed_records_per_segment_[segment_size_idx];

    // Use the model to determine how many records to place in the segment,
    // based on the desired goal. We use binary search against the model to
    // minimize the effect of precision errors.
    const auto cutoff_it = std::lower_bound(
        records_processed.begin(), records_processed.end(), records_in_segment,
        [&dataset, &maybe_line, &base_key](const size_t rec_idx_a,
                                           const size_t recs_in_segment) {
          const Key key_a = dataset[rec_idx_a].first;
          const auto pos_a = maybe_line->line()(key_a - base_key);
          return pos_a < recs_in_segment;
        });
    assert(cutoff_it != records_processed.begin());
    size_t extra_keys = records_processed.end() - cutoff_it;
    while (extra_keys > 0) {
      records_processed.pop_back();
      extra_keys--;
      next_idx--;
    }
    segments.push_back(Segment::MultiPage(
        segment_size, records_processed.front(), records_processed.back() + 1,
        plr::BoundedLine64(maybe_line->line().Rescale(records_per_page_goal_),
                           0, records_processed.size() - 1)));
  }

  return segments;
}

}  // namespace pg
}  // namespace llsm
