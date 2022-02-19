#include "segment_index.h"

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

SegmentIndex::Entry& SegmentIndex::SegmentForKeyImpl(const Key key) {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return *it;
}

const SegmentIndex::Entry& SegmentIndex::SegmentForKeyImpl(const Key key) const {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return *it;
}

}  // namespace pg
}  // namespace llsm
