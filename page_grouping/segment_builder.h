#pragma once

#include <cstdlib>
#include <deque>
#include <utility>
#include <vector>

#include "key.h"
#include "llsm/slice.h"
#include "plr/data.h"
#include "plr/greedy.h"

namespace llsm {
namespace pg {

// Stores information about the built segments. The indices in this struct refer
// to the dataset passed in to `SegmentBuilder::BuildFromDataset()`.
struct DatasetSegment {
 public:
  static DatasetSegment MultiPage(size_t page_count, size_t start_idx,
                                  size_t end_idx, plr::BoundedLine64 model);
  static DatasetSegment SinglePage(size_t start_idx, size_t end_idx);

  size_t page_count;
  // Start and end index (relative to the dataset) of the records that should be
  // included in this segment. The start index is inclusive, the end index is
  // exclusive.
  size_t start_idx, end_idx;
  // Will be empty when `page_count == 1`.
  std::optional<plr::BoundedLine64> model;
};

struct Segment {
 public:
  size_t page_count;
  // The records in the segment in sorted order.
  std::vector<std::pair<Key, Slice>> records;
  // Will be empty when `page_count == 1`.
  std::optional<plr::BoundedLine64> model;
};

class SegmentBuilder {
 public:
  SegmentBuilder(const size_t records_per_page_goal,
                 const size_t records_per_page_delta);

  // A first-pass greedy segment builder implementation. For simplicity, this
  // implementation assumes the key dataset can fit in memory. But
  // fundamentally, the algorithm does not require the entire dataset to be
  // materialized in memory (e.g., it could operate on a stream of sorted keys).
  std::vector<DatasetSegment> BuildFromDataset(
      const std::vector<std::pair<Key, Slice>>& dataset);

  // Stream-based builder interface. Offer the builder one record at a time.
  std::vector<Segment> Offer(std::pair<Key, Slice> record);
  std::vector<Segment> Finish();
  void ResetStream();

  // The number of pages in each segment.
  // The index of the vector represents the segment "type", and its value
  // represents the number of pages in the segment.
  static const std::vector<size_t> kSegmentPageCounts;

  // Maps the page count to the segment "index" (in `kSegmentPageCounts`).
  static const std::unordered_map<size_t, size_t> kPageCountToSegment;

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

  size_t records_per_page_goal_, records_per_page_delta_;
  size_t max_records_in_segment_;
  std::vector<size_t> allowed_records_per_segment_;

  // Used by the stream-based builder.
  enum class State { kNeedBase, kHasBase, kFillingSinglePage };
  State state_;
  std::optional<plr::GreedyPLRBuilder64> plr_;
  Key base_key_;
  std::deque<std::pair<Key, Slice>> processed_records_;
};

}  // namespace pg
}  // namespace llsm
