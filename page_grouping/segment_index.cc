#include "segment_index.h"

#include <algorithm>

namespace llsm {
namespace pg {

SegmentIndex::Entry SegmentIndex::SegmentForKey(const Key key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // Return a copy.
  return SegmentForKeyImpl(key);
}

std::optional<SegmentIndex::Entry> SegmentIndex::NextSegmentForKey(
    const Key key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = index_.upper_bound(key);
  if (it == index_.end()) {
    return std::optional<Entry>();
  }
  // Return a copy.
  return *it;
}

void SegmentIndex::SetSegmentOverflow(const Key key, bool overflow) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto& entry = SegmentForKeyImpl(key);
  entry.second.SetOverflow(overflow);
}

std::vector<SegmentIndex::Entry> SegmentIndex::FindRewriteRegion(
    const Key segment_base) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  const auto it = index_.lower_bound(segment_base);
  assert(it != index_.end());
  std::vector<Entry> segments_to_rewrite = {*it};

  // Scan backward.
  if (it != index_.begin()) {
    auto prev_it(it);
    while (true) {
      --prev_it;
      if (!prev_it->second.HasOverflow()) break;
      segments_to_rewrite.emplace_back(*prev_it);
      if (prev_it == index_.begin()) break;
    }
  }

  // Scan forward.
  auto next_it(it);
  ++next_it;
  for (; next_it != index_.end(); ++next_it) {
    if (!next_it->second.HasOverflow()) break;
    segments_to_rewrite.emplace_back(*next_it);
  }

  // Sort the segments.
  std::sort(segments_to_rewrite.begin(), segments_to_rewrite.end(),
            [](const std::pair<Key, SegmentInfo>& seg1,
               const std::pair<Key, SegmentInfo>& seg2) {
              return seg1.first < seg2.first;
            });

  return segments_to_rewrite;
}

SegmentIndex::Entry& SegmentIndex::SegmentForKeyImpl(const Key key) {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return *it;
}

const SegmentIndex::Entry& SegmentIndex::SegmentForKeyImpl(
    const Key key) const {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return *it;
}

}  // namespace pg
}  // namespace llsm
