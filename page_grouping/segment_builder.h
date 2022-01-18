#pragma once

#include <cstdlib>
#include <vector>
#include <utility>

#include "llsm/slice.h"
#include "manager.h"

namespace llsm {
namespace pg {

struct Segment {
 public:
  size_t page_count;
  std::vector<size_t> record_indices;
};

class SegmentBuilder {
 public:
  SegmentBuilder(const size_t records_per_page_goal,
                 const size_t records_per_page_delta);

  std::vector<Segment> Build(const std::vector<std::pair<Key, const Slice>>& dataset);

 private:
  size_t records_per_page_goal_, records_per_page_delta_;
  size_t max_records_in_segment_;
  std::vector<size_t> allowed_records_per_segment_;
};

}  // namespace pg
}  // namespace llsm
