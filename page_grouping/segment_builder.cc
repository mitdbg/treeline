#include "segment_builder.h"

#include <algorithm>
#include <limits>

#include "manager.h"
#include "plr/data.h"
#include "plr/greedy.h"
#include "plr/pgm.h"

namespace tl {
namespace pg {

// The number of pages in each segment. If you change this, change
// `PageCountToSegment()` below too.
const std::vector<size_t>& SegmentBuilder::SegmentPageCounts() {
  // NOTE: We construct and initialize the vector here to avoid the static
  // initialization order fiasco.
  // https://en.cppreference.com/w/cpp/language/siof
  static const std::vector<size_t> kSegmentPageCounts = {1, 2, 4, 8, 16};
  return kSegmentPageCounts;
}

const std::unordered_map<size_t, size_t>& SegmentBuilder::PageCountToSegment() {
  // NOTE: We construct and initialize the vector here to avoid the static
  // initialization order fiasco.
  // https://en.cppreference.com/w/cpp/language/siof
  static const std::unordered_map<size_t, size_t> kPageCountToSegment = {
      {1, 0}, {2, 1}, {4, 2}, {8, 3}, {16, 4}};
  return kPageCountToSegment;
}

// The maximum number of pages in any segment.
static const size_t kMaxSegmentSize =
    tl::pg::SegmentBuilder::SegmentPageCounts().back();

// This value is a `double`'s maximum representable integer. The PLR algorithm
// uses `double`s internally and we need the inputs to the PLR (which are
// integers) to be representable as `double`s.
//
// Note that this restriction only affects extremely sparse key spaces (e.g.,
// when the difference between keys exceeds 2^53). This is because we build
// linear models over the *differences* between the keys in the segment and the
// segment's lower bound.
static constexpr Key kMaxKeyDiff = 1ULL << std::numeric_limits<double>::digits;

SegmentBuilder::SegmentBuilder(const size_t records_per_page_goal,
                               const double records_per_page_epsilon,
                               Strategy strategy)
    : records_per_page_goal_(records_per_page_goal),
      records_per_page_epsilon_(records_per_page_epsilon),
      max_records_in_segment_(
          (records_per_page_goal_ + records_per_page_epsilon_) * kMaxSegmentSize),
      state_(State::kNeedBase),
      strategy_(strategy),
      plr_(nullptr),
      base_key_(0) {
  allowed_records_per_segment_.reserve(SegmentPageCounts().size());
  for (size_t pages : SegmentPageCounts()) {
    allowed_records_per_segment_.push_back(pages * records_per_page_goal_);
  }
}

std::vector<Segment> SegmentBuilder::BuildFromDataset(
    const std::vector<std::pair<Key, Slice>>& dataset, bool force_add_min_key) {
  // Precondition: The dataset is sorted by key in ascending order.
  std::vector<Segment> segments;
  ResetStream();
  if (force_add_min_key) {
    Offer({Manager::kMinReservedKey, Slice()});
  }
  for (const auto& rec : dataset) {
    auto segs = Offer(rec);
    segments.insert(segments.end(), std::make_move_iterator(segs.begin()),
                    std::make_move_iterator(segs.end()));
  }
  auto segs = Finish();
  segments.insert(segments.end(), std::make_move_iterator(segs.begin()),
                  std::make_move_iterator(segs.end()));
  return segments;
}

std::vector<Segment> SegmentBuilder::Offer(std::pair<Key, Slice> record) {
  static const std::vector<Segment> kNoSegments = {};
  static const std::optional<plr::BoundedLine64> kNoModel =
      std::optional<plr::BoundedLine64>();

  // Precondition: The records are offered in sorted order (sorted by key in
  // ascending order).
  if (state_ == State::kNeedBase) {
    assert(processed_records_.empty());
    base_key_ = record.first;
    if (strategy_ == Strategy::kGreedy) {
      plr_ = std::make_unique<plr::GreedyPLRBuilder64>(records_per_page_epsilon_);
    } else if (strategy_ == Strategy::kPGM) {
      plr_ = std::make_unique<plr::PGMBuilder<double>>(records_per_page_epsilon_);
    } else {
      assert(false);
    }
    auto maybe_line = plr_->Offer(plr::Point64(0, 0));
    // Only one point.
    assert(!maybe_line.has_value());
    processed_records_.push_back(std::move(record));
    state_ = State::kHasBase;
    return kNoSegments;

  } else if (state_ == State::kHasBase) {
    std::optional<plr::BoundedLine64> line;
    const Key diff = record.first - base_key_;
    if (processed_records_.size() < max_records_in_segment_ &&
        diff <= kMaxKeyDiff) {
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
      // We exceeded the number of records per segment or adding this record
      // would produce a key difference that cannot be represented exactly by a
      // `double` (which we want to avoid).
      line = plr_->Finish();
      // Note that `line.has_value() == false` is possible here. This happens if
      // the PLR builder has only seen one record.
    }

    // Figure out how big of a segment we can make, according to the model. If
    // there is no model (see the comments above), we just try to fill one page
    // regardless.
    const int segment_size_idx =
        line.has_value() ? ComputeSegmentSizeIndex(*line) : -1;

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

    const size_t segment_size = SegmentPageCounts()[segment_size_idx];
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

    const size_t segment_size = SegmentPageCounts()[segment_size_idx];
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
  const size_t segment_size = SegmentPageCounts()[segment_size_idx];
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
  s.base_key = base_key_;
  s.page_count = page_count;
  s.model = std::move(model);
  s.records.reserve(std::min(num_records, processed_records_.size()));
  for (size_t i = 0; i < num_records && !processed_records_.empty(); ++i) {
    if (i == 0) {
      assert(processed_records_.front().first == base_key_);
    }
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

std::optional<Key> SegmentBuilder::CurrentBaseKey() const {
  if (state_ == State::kNeedBase) {
    return std::optional<Key>();
  }
  assert(!processed_records_.empty());
  assert(processed_records_.front().first == base_key_);
  return base_key_;
}

}  // namespace pg
}  // namespace tl
