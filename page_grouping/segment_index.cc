#include "segment_index.h"

#include <algorithm>

#include "rand_exp_backoff.h"

namespace {

constexpr uint32_t kBackoffSaturate = 12;

}  // namespace

namespace llsm {
namespace pg {

SegmentIndex::SegmentIndex(std::shared_ptr<LockManager> lock_manager)
    : lock_manager_(std::move(lock_manager)) {
  assert(lock_manager_ != nullptr);
}

SegmentIndex::Entry SegmentIndex::SegmentForKey(const Key key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return IndexIteratorToEntry(SegmentForKeyImpl(key));
}

SegmentIndex::Entry SegmentIndex::SegmentForKeyWithLock(
    const Key key, LockManager::SegmentMode mode) const {
  RandExpBackoff backoff(kBackoffSaturate);
  while (true) {
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      const auto it = SegmentForKeyImpl(key);
      const bool lock_granted =
          lock_manager_->TryAcquireSegmentLock(it->second.id(), mode);
      if (lock_granted) {
        return IndexIteratorToEntry(it);
      }
    }
    backoff.Wait();
  }
}

std::optional<SegmentIndex::Entry> SegmentIndex::NextSegmentForKey(
    const Key key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = index_.upper_bound(key);
  if (it == index_.end()) {
    return std::optional<Entry>();
  }
  // Return a copy.
  return IndexIteratorToEntry(it);
}

std::optional<SegmentIndex::Entry> SegmentIndex::NextSegmentForKeyWithLock(
    const Key key, LockManager::SegmentMode mode) const {
  RandExpBackoff backoff(kBackoffSaturate);
  while (true) {
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      const auto it = index_.upper_bound(key);
      if (it == index_.end()) {
        return std::optional<Entry>();
      }
      const bool lock_granted =
          lock_manager_->TryAcquireSegmentLock(it->second.id(), mode);
      if (lock_granted) {
        // Returns a copy.
        return IndexIteratorToEntry(it);
      }
    }
    backoff.Wait();
  }
}

void SegmentIndex::SetSegmentOverflow(const Key key, bool overflow) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = SegmentForKeyImpl(key);
  it->second.SetOverflow(overflow);
}

std::vector<std::pair<Key, SegmentInfo>> SegmentIndex::FindRewriteRegion(
    const Key segment_base) const {
  std::vector<std::pair<Key, SegmentInfo>> segments_to_rewrite;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto it = index_.lower_bound(segment_base);
    assert(it != index_.end());
    segments_to_rewrite.emplace_back(*it);

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
  }

  // Sort the segments.
  std::sort(segments_to_rewrite.begin(), segments_to_rewrite.end(),
            [](const std::pair<Key, SegmentInfo>& seg1,
               const std::pair<Key, SegmentInfo>& seg2) {
              return seg1.first < seg2.first;
            });
  return segments_to_rewrite;
}

std::pair<Key, Key> SegmentIndex::GetSegmentBoundsFor(const Key key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = SegmentForKeyImpl(key);
  const Key lower = it->first;
  ++it;
  if (it == index_.end()) {
    return {lower, std::numeric_limits<Key>::max()};
  } else {
    return {lower, it->first};
  }
}

SegmentIndex::OrderedMap::iterator SegmentIndex::SegmentForKeyImpl(
    const Key key) {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return it;
}

SegmentIndex::OrderedMap::const_iterator SegmentIndex::SegmentForKeyImpl(
    const Key key) const {
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }
  return it;
}

SegmentIndex::Entry SegmentIndex::IndexIteratorToEntry(
    SegmentIndex::OrderedMap::const_iterator it) const {
  // We deliberately make a copy.
  Entry entry;
  entry.lower = it->first;
  entry.sinfo = it->second;
  ++it;
  if (it == index_.end()) {
    entry.upper = std::numeric_limits<Key>::max();
  } else {
    entry.upper = it->first;
  }
  return entry;
}

}  // namespace pg
}  // namespace llsm
