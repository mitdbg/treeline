#pragma once

#include <cstdlib>
#include <utility>
#include <vector>

#include "llsm/slice.h"
#include "manager.h"
#include "plr/data.h"

namespace llsm {
namespace pg {

struct Segment {
 public:
  static Segment MultiPage(size_t page_count, size_t start_idx, size_t end_idx,
                           plr::BoundedLine64 model);
  static Segment SinglePage(size_t start_idx, size_t end_idx);

  size_t page_count;
  // Start and end index (relative to the dataset) of the records that should be
  // included in this segment. The start index is inclusive, the end index is
  // exclusive.
  size_t start_idx, end_idx;
  // Will be empty when `page_count == 1`.
  std::optional<plr::BoundedLine64> model;
};

// A first-pass greedy segment builder implementation. For simplicity, this
// implementation assumes the key dataset can fit in memory. But fundamentally,
// the algorithm does not require the entire dataset to be materialized in
// memory (e.g., it could operate on a stream of sorted keys).
class SegmentBuilder {
 public:
  SegmentBuilder(const size_t records_per_page_goal,
                 const size_t records_per_page_delta);

  std::vector<Segment> Build(const std::vector<std::pair<Key, Slice>>& dataset);

  // The number of pages in each segment.
  // The index of the vector represents the segment "type", and its value
  // represents the number of pages in the segment.
  static const std::vector<size_t> kSegmentPageCounts;

  // Maps the page count to the segment "index" (in `kSegmentPageCounts`).
  static const std::unordered_map<size_t, size_t> kPageCountToSegment;

 private:
  size_t records_per_page_goal_, records_per_page_delta_;
  size_t max_records_in_segment_;
  std::vector<size_t> allowed_records_per_segment_;
};

}  // namespace pg
}  // namespace llsm
