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
  static Segment MultiPage(size_t page_count, std::vector<size_t> indices,
                           plr::BoundedLine64 model);
  static Segment SinglePage(std::vector<size_t> indices);

  size_t page_count;
  std::vector<size_t> record_indices;
  // Will be empty when `page_count == 1`.
  std::optional<plr::BoundedLine64> model;
};

class SegmentBuilder {
 public:
  SegmentBuilder(const size_t records_per_page_goal,
                 const size_t records_per_page_delta);

  std::vector<Segment> Build(
      const std::vector<std::pair<Key, Slice>>& dataset);

 private:
  size_t records_per_page_goal_, records_per_page_delta_;
  size_t max_records_in_segment_;
  std::vector<size_t> allowed_records_per_segment_;
};

}  // namespace pg
}  // namespace llsm
