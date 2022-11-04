#pragma once

#include <cstdlib>
#include <deque>
#include <unordered_map>
#include <utility>
#include <vector>

#include "key.h"
#include "plr/data.h"
#include "plr/greedy.h"
#include "treeline/slice.h"

namespace tl {
namespace pg {

// Stores information about the built segments.
struct Segment {
  Key base_key;
  size_t page_count;
  // The records in the segment in sorted order.
  std::vector<std::pair<Key, Slice>> records;
  // Will be empty when `page_count == 1`.
  std::optional<plr::BoundedLine64> model;
};

class SegmentBuilder {
 public:
  enum class Strategy {
    kGreedy,
    kPGM,
  };

  SegmentBuilder(const size_t records_per_page_goal,
                 const double records_per_page_epsilon,
                 Strategy strategy = Strategy::kPGM);

  // Build segments when the entire dataset can fit in memory.
  //
  // If `force_add_min_key` is true then this method will add a placeholder
  // record with the key `Manager::kMinReservedKey` and an empty value.
  std::vector<Segment> BuildFromDataset(const std::vector<Record>& dataset,
                                        bool force_add_min_key = false);

  // Stream-based builder interface. Offer the builder one record at a time.
  std::vector<Segment> Offer(std::pair<Key, Slice> record);
  std::vector<Segment> Finish();
  void ResetStream();
  // Returns the smallest key in the next segment to be emitted by this builder.
  std::optional<Key> CurrentBaseKey() const;

  // The number of pages in each segment.
  // The index of the vector represents the segment "type", and its value
  // represents the number of pages in the segment.
  static const std::vector<size_t>& SegmentPageCounts();

  // Maps the page count to the segment "index" (in `SegmentPageCounts()`).
  static const std::unordered_map<size_t, size_t>& PageCountToSegment();

 private:
  // Helper methods used by the stream-based builder interface.
  std::vector<Segment> DrainRemainingRecordsAndReset(
      std::vector<Segment> to_return, std::pair<Key, Slice> record);
  std::vector<Segment> DrainRemainingRecordsAndReset(
      std::vector<Segment> to_return);
  int ComputeSegmentSizeIndex(const plr::BoundedLine64& model) const;
  size_t ComputeNumRecordsInSegment(const plr::BoundedLine64& model,
                                    const int segment_size_idx) const;
  Segment CreateSegmentUsing(std::optional<plr::BoundedLine64> model,
                             size_t page_count, size_t num_records);

  size_t records_per_page_goal_;
  double records_per_page_epsilon_;
  size_t max_records_in_segment_;
  std::vector<size_t> allowed_records_per_segment_;

  // Used by the stream-based builder.
  enum class State { kNeedBase, kHasBase, kFillingSinglePage };
  State state_;
  Strategy strategy_;
  std::unique_ptr<plr::PLRBuilder<double>> plr_;
  Key base_key_;
  std::deque<std::pair<Key, Slice>> processed_records_;
};

}  // namespace pg
}  // namespace tl
