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
  // Return a copy.
  return SegmentForKeyImpl(key);
}

SegmentIndex::Entry SegmentIndex::SegmentForKeyWithLock(
    const Key key, LockManager::SegmentMode mode) const {
  RandExpBackoff backoff(kBackoffSaturate);
  while (true) {
    {
      std::shared_lock<std::shared_mutex> lock(mutex_);
      auto& entry = SegmentForKeyImpl(key);
      const bool lock_granted =
          lock_manager_->TryAcquireSegmentLock(entry.second.id(), mode);
      if (lock_granted) {
        // Returns a copy.
        return entry;
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
  return *it;
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
        return *it;
      }
    }
    backoff.Wait();
  }
}

void SegmentIndex::SetSegmentOverflow(const Key key, bool overflow) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto& entry = SegmentForKeyImpl(key);
  entry.second.SetOverflow(overflow);
}

std::vector<SegmentIndex::Entry> SegmentIndex::FindRewriteRegion(
    const Key segment_base) const {
  std::vector<Entry> segments_to_rewrite;
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
