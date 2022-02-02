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

DatasetSegment DatasetSegment::MultiPage(size_t page_count, size_t start_idx,
                                         size_t end_idx,
                                         plr::BoundedLine64 model) {
  DatasetSegment s;
  s.page_count = page_count;
  s.start_idx = start_idx;
  s.end_idx = end_idx;
  s.model = model;
  return s;
}

DatasetSegment DatasetSegment::SinglePage(size_t start_idx, size_t end_idx) {
  DatasetSegment s;
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
          (records_per_page_goal_ + records_per_page_delta_) * kMaxSegmentSize),
      state_(State::kNeedBase),
      plr_(),
      base_key_(0) {
  allowed_records_per_segment_.reserve(kSegmentPageCounts.size());
  for (size_t pages : kSegmentPageCounts) {
    allowed_records_per_segment_.push_back(pages * records_per_page_goal_);
  }
}

std::vector<DatasetSegment> SegmentBuilder::BuildFromDataset(
    const std::vector<std::pair<Key, Slice>>& dataset) const {
  // Precondition: The dataset is sorted by key in ascending order.
  size_t next_idx = 0;
  std::vector<size_t> records_processed;
  std::vector<DatasetSegment> segments;
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
        segments.push_back(DatasetSegment::SinglePage(
            records_processed.front(), records_processed.back() + 1));
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
    // We want to find the index of the *largest* value that is less than or
    // equal to `last_record_size`.
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
          records_processed.pop_back();
          --next_idx;
          --extra_keys;
        }
      }
      segments.push_back(DatasetSegment::SinglePage(
          records_processed.front(), records_processed.back() + 1));
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

    Key last_key = std::numeric_limits<Key>::max();
    if (next_idx < dataset.size()) {
      last_key = dataset[next_idx].first;
    }
    segments.push_back(DatasetSegment::MultiPage(
        segment_size, records_processed.front(), records_processed.back() + 1,
        plr::BoundedLine64(maybe_line->line().Rescale(records_per_page_goal_),
                           /*start_x=*/0, /*end_x=*/last_key - base_key)));
  }

  return segments;
}

std::vector<Segment> SegmentBuilder::Offer(std::pair<Key, Slice> record) {
  static const std::vector<Segment> kNoSegments = {};
  static const std::optional<plr::BoundedLine64> kNoModel =
      std::optional<plr::BoundedLine64>();

  // Precondition: The records are offered in sorted order (sorted by key in
  // ascending order).
  if (state_ == State::kNeedBase) {
    base_key_ = record.first;
    plr_ = plr::GreedyPLRBuilder64(records_per_page_delta_);
    auto maybe_line = plr_->Offer(plr::Point64(0, 0));
    // Only one point.
    assert(!maybe_line.has_value());
    processed_records_.push_back(std::move(record));
    state_ = State::kHasBase;
    return kNoSegments;

  } else if (state_ == State::kHasBase) {
    std::optional<plr::BoundedLine64> line;
    if (processed_records_.size() < max_records_in_segment_) {
      const Key diff = record.first - base_key_;
      // This algorithm assumes equally sized keys. So we give each record
      // equal weight; a straightfoward approach is just to count up by 1.
      line = plr_->Offer(plr::Point64(diff, processed_records_.size()));
      if (!line.has_value()) {
        // Can absorb the record into the current model.
        processed_records_.push_back(std::move(record));
        return kNoSegments;
      }
      // Otherwise, we cannot include the current record without violating the
      // error threshold. This current record will go into the next "batch".

    } else {
      // We exceeded the number of records per segment.
      line = plr_->Finish();
      if (!line.has_value()) {
        // This only happens if the PLR builder has only seen one point. This is
        // a degenerate case that occurs when `max_records_in_segment_ == 1`.
        assert(processed_records_.size() == 1);
        assert(max_records_in_segment_ == 1);
        std::vector<Segment> results = {CreateSegmentUsing(kNoModel,
                                                           /*page_count=*/1,
                                                           /*num_records=*/1)};
        return DrainRemainingRecordsAndReset(std::move(results),
                                             std::move(record));
      }
    }

    // Figure out how big of a segment we can make, according to the model.
    const int segment_size_idx = ComputeSegmentSizeIndex(*line);

    if (segment_size_idx <= 0) {
      // One of two cases:
      // - Could not build a model that "covers" one full page
      // - Could not build a model that "covers" more than one page
      // So we just fill one page regardless and omit the model.
      const size_t target_size = allowed_records_per_segment_[0];
      if (target_size > processed_records_.size()) {
        // Can still add more records into this page.
        processed_records_.push_back(std::move(record));
        state_ = State::kFillingSinglePage;
        return kNoSegments;
      }

      // Build a segment result from the records collected up to here.
      std::vector<Segment> results = {
          CreateSegmentUsing(kNoModel, /*page_count=*/1, target_size)};
      return DrainRemainingRecordsAndReset(std::move(results),
                                           std::move(record));
    }

    const size_t segment_size = kSegmentPageCounts[segment_size_idx];
    const size_t actual_records_in_segment =
        ComputeNumRecordsInSegment(*line, segment_size_idx);

    // start_x/end_x are unused, so we put dummy values.
    auto model =
        plr::BoundedLine64(line->line().Rescale(records_per_page_goal_),
                           /*start_x=*/0, /*end_x=*/1);
    std::vector<Segment> segments = {
        CreateSegmentUsing(std::move(model), /*page_count=*/segment_size,
                           actual_records_in_segment)};
    return DrainRemainingRecordsAndReset(std::move(segments),
                                         std::move(record));

  } else if (state_ == State::kFillingSinglePage) {
    const size_t target_size = allowed_records_per_segment_[0];
    if (target_size > processed_records_.size()) {
      processed_records_.push_back(std::move(record));
      return kNoSegments;
    }
    std::vector<Segment> results = {
        CreateSegmentUsing(kNoModel, /*page_count=*/1, target_size)};
    return DrainRemainingRecordsAndReset(std::move(results), std::move(record));

  } else {
    // This branch should be unreachable.
    throw std::runtime_error("Unknown state.");
  }
}

std::vector<Segment> SegmentBuilder::Finish() {
  static const std::optional<plr::BoundedLine64> kNoModel =
      std::optional<plr::BoundedLine64>();

  if (state_ == State::kNeedBase) {
    // No additional segments.
    ResetStream();
    return {};

  } else if (state_ == State::kHasBase) {
    const auto line = plr_->Finish();
    if (!line.has_value()) {
      // Only one record.
      assert(processed_records_.size() == 1);
      std::vector<Segment> results = {
          CreateSegmentUsing(kNoModel, /*page_count=*/1, /*num_records=*/1)};
      ResetStream();
      return results;
    }

    const int segment_size_idx = ComputeSegmentSizeIndex(*line);
    if (segment_size_idx <= 0) {
      // Fill one page and omit the model.
      const size_t target_size = allowed_records_per_segment_[0];

      // Build a segment result from the records collected up to here.
      std::vector<Segment> results = {
          CreateSegmentUsing(kNoModel, /*page_count=*/1, target_size)};

      // Recursively finish processing any leftover records.
      results = DrainRemainingRecordsAndReset(std::move(results));
      auto additional_segments = Finish();
      results.insert(results.end(),
                     std::make_move_iterator(additional_segments.begin()),
                     std::make_move_iterator(additional_segments.end()));
      return results;
    }

    const size_t segment_size = kSegmentPageCounts[segment_size_idx];
    const size_t actual_records_in_segment =
        ComputeNumRecordsInSegment(*line, segment_size_idx);

    // start_x/end_x are unused, so we put dummy values.
    auto model =
        plr::BoundedLine64(line->line().Rescale(records_per_page_goal_),
                           /*start_x=*/0, /*end_x=*/1);
    std::vector<Segment> segments = {CreateSegmentUsing(
        std::move(model),
        /*page_count=*/segment_size, actual_records_in_segment)};

    // Recursively finish processing any leftover records.
    segments = DrainRemainingRecordsAndReset(std::move(segments));
    auto additional_segments = Finish();
    segments.insert(segments.end(), additional_segments.begin(),
                    additional_segments.end());
    return segments;

  } else if (state_ == State::kFillingSinglePage) {
    const size_t target_size = allowed_records_per_segment_[0];
    assert(processed_records_.size() <= target_size);
    std::vector<Segment> results = {
        CreateSegmentUsing(kNoModel, /*page_count=*/1, target_size)};
    ResetStream();
    return results;

  } else {
    throw std::runtime_error("Unknown state.");
  }
}

std::vector<Segment> SegmentBuilder::DrainRemainingRecordsAndReset(
    std::vector<Segment> to_return, std::pair<Key, Slice> record) {
  std::vector<std::pair<Key, Slice>> leftover_records(
      std::make_move_iterator(processed_records_.begin()),
      std::make_move_iterator(processed_records_.end()));
  leftover_records.push_back(std::move(record));
  ResetStream();
  for (auto& rec : leftover_records) {
    auto res = Offer(std::move(rec));
    to_return.insert(to_return.end(), std::make_move_iterator(res.begin()),
                     std::make_move_iterator(res.end()));
  }
  return to_return;
}

std::vector<Segment> SegmentBuilder::DrainRemainingRecordsAndReset(
    std::vector<Segment> to_return) {
  std::vector<std::pair<Key, Slice>> leftover_records(
      std::make_move_iterator(processed_records_.begin()),
      std::make_move_iterator(processed_records_.end()));
  ResetStream();
  for (auto& rec : leftover_records) {
    auto res = Offer(std::move(rec));
    to_return.insert(to_return.end(), std::make_move_iterator(res.begin()),
                     std::make_move_iterator(res.end()));
  }
  return to_return;
}

int SegmentBuilder::ComputeSegmentSizeIndex(
    const plr::BoundedLine64& model) const {
  // According to the generated model, how many records have we processed
  // when we hit the last key?
  const double last_record_size =
      1.0 +
      std::max(0.0, model.line()(processed_records_.back().first - base_key_));

  // Find the largest possible segment we can make.
  // `allowed_records_per_segment_` contains the ideal size for each
  // segment. We want to find the index of the *largest* value that is less
  // than or equal to `last_record_size`.
  const auto segment_size_idx_it =
      std::upper_bound(allowed_records_per_segment_.begin(),
                       allowed_records_per_segment_.end(), last_record_size);
  const int segment_size_idx =
      (segment_size_idx_it - allowed_records_per_segment_.begin()) - 1;
  return segment_size_idx;
}

size_t SegmentBuilder::ComputeNumRecordsInSegment(
    const plr::BoundedLine64& model, const int segment_size_idx) const {
  // Compute how many records can we actually fit in the segment based on the
  // model.
  const size_t segment_size = kSegmentPageCounts[segment_size_idx];
  assert(segment_size > 1);

  const size_t records_in_segment =
      allowed_records_per_segment_[segment_size_idx];

  // Use the model to determine how many records to place in the segment,
  // based on the desired goal. We use binary search against the model to
  // minimize the effect of precision errors.
  const auto cutoff_it = std::lower_bound(
      processed_records_.begin(), processed_records_.end(), records_in_segment,
      [this, &model](const std::pair<Key, Slice>& rec,
                     const size_t recs_in_segment) {
        const Key key_a = rec.first;
        const auto pos_a = model.line()(key_a - base_key_);
        return pos_a < recs_in_segment;
      });
  assert(cutoff_it != processed_records_.begin());
  const size_t actual_records_in_segment =
      cutoff_it - processed_records_.begin();
  return actual_records_in_segment;
}

Segment SegmentBuilder::CreateSegmentUsing(
    std::optional<plr::BoundedLine64> model, size_t page_count,
    size_t num_records) {
  Segment s;
  s.page_count = page_count;
  s.model = std::move(model);
  s.records.reserve(std::min(num_records, processed_records_.size()));
  for (size_t i = 0; i < num_records && !processed_records_.empty(); ++i) {
    s.records.push_back(std::move(processed_records_.front()));
    processed_records_.pop_front();
  }
  return s;
}

void SegmentBuilder::ResetStream() {
  processed_records_.clear();
  state_ = State::kNeedBase;
  plr_.reset();
  base_key_ = 0;
}

}  // namespace pg
}  // namespace llsm
