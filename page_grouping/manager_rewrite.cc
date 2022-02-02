#include <utility>
#include <vector>

#include "manager.h"

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
  // TODO
}

}  // namespace pg
}  // namespace llsm
