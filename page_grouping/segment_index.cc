#include "segment_index.h"

#include <algorithm>

#include "rand_exp_backoff.h"

namespace {

constexpr uint32_t kBackoffSaturate = 12;

}  // namespace

namespace tl {
namespace pg {

SegmentIndex::SegmentIndex(std::shared_ptr<LockManager> lock_manager)
    : lock_manager_(std::move(lock_manager)),
      bytes_allocated_(0),
      index_(TrackingAllocator<std::pair<Key, SegmentInfo>>(bytes_allocated_)) {
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

std::vector<SegmentIndex::Entry> SegmentIndex::FindAndLockRewriteRegion(
    const Key segment_base, const uint32_t search_radius) const {
  std::vector<SegmentIndex::Entry> segments_to_rewrite;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto it = index_.lower_bound(segment_base);
    assert(it != index_.end());
    segments_to_rewrite.emplace_back(IndexIteratorToEntry(it));

    // Scan backward.
    if (it != index_.begin()) {
      auto prev_it(it);
      uint32_t num_to_check = search_radius;
      while (num_to_check > 0) {
        --prev_it;
        --num_to_check;
        if (!prev_it->second.HasOverflow()) break;
        segments_to_rewrite.emplace_back(IndexIteratorToEntry(prev_it));
        if (prev_it == index_.begin()) break;
      }
    }

    // Scan forward.
    auto next_it(it);
    ++next_it;
    for (uint32_t num_to_check = search_radius;
         num_to_check > 0 && next_it != index_.end();
         ++next_it, --num_to_check) {
      if (!next_it->second.HasOverflow()) break;
      segments_to_rewrite.emplace_back(IndexIteratorToEntry(next_it));
    }
  }
  assert(!segments_to_rewrite.empty());

  // Sort the segments.
  std::sort(segments_to_rewrite.begin(), segments_to_rewrite.end(),
            [](const auto& seg1, const auto& seg2) {
              return seg1.lower < seg2.lower;
            });

  const bool succeeded = LockSegmentsForRewrite(segments_to_rewrite);
  if (!succeeded) {
    segments_to_rewrite.clear();
  }

  return segments_to_rewrite;
}

std::optional<std::vector<SegmentIndex::Entry>>
SegmentIndex::FindAndLockNextOverflowRegion(const Key start_key,
                                            const Key end_key) const {
  std::vector<Entry> overflow_region;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // Find the segment that contains `start_key`. The index stores segment
    // lower boundaries (inclusive).
    auto it = index_.upper_bound(start_key);
    if (it != index_.begin()) {
      --it;
    }

    while (it != index_.end() && it->first < end_key &&
           !it->second.HasOverflow()) {
      ++it;
    }
    if (it == index_.end() || it->first >= end_key) {
      return overflow_region;
    }
    do {
      overflow_region.emplace_back(IndexIteratorToEntry(it));
      ++it;
    } while (it != index_.end() && it->first < end_key &&
             it->second.HasOverflow());
  }

  if (overflow_region.empty()) {
    return overflow_region;
  }

  // By construction, the segments in `overflow_region` are sorted.
  const bool succeeded = LockSegmentsForRewrite(overflow_region);
  if (!succeeded) {
    // Empty optional to indicate that the caller should retry.
    return std::optional<std::vector<Entry>>();
  }
  return overflow_region;
}

bool SegmentIndex::LockSegmentsForRewrite(
    const std::vector<SegmentIndex::Entry>& segments_to_lock) const {
  // There is an unlikely pathological case where the segments we are trying to
  // lock for a rewrite end up being reused in different parts of the key space.
  // To avoid causing a deadlock, we attempt to acquire the segment lock a
  // maximum number of times.
  static constexpr size_t kMaxAttempts = 1000;

  // Acquire locks in order. We do not hold the index latch while doing this
  // because acquiring reorg locks may take time.
  RandExpBackoff backoff(kBackoffSaturate);
  size_t num_granted = 0;
  for (const auto& seg : segments_to_lock) {
    backoff.Reset();
    bool lock_granted = false;
    for (size_t attempt = 0; attempt < kMaxAttempts; ++attempt) {
      lock_granted = lock_manager_->TryAcquireSegmentLock(
          seg.sinfo.id(), LockManager::SegmentMode::kReorg);
      if (lock_granted) break;
      backoff.Wait();
    }

    // We exceeded the number of acquisition attempts. Release all granted locks
    // and retry.
    if (!lock_granted) {
      for (size_t i = 0; i < num_granted; ++i) {
        lock_manager_->ReleaseSegmentLock(segments_to_lock[i].sinfo.id(),
                                          LockManager::SegmentMode::kReorg);
      }
      return false;
    }

    // Keep track of the number of locks we were granted in case we need to
    // abort and retry.
    ++num_granted;
  }

  // Check that the locked segments are still valid. All segments should exist
  // and their lower bounds should be consistent. The segments may be invalid if
  // another reorg intervened.
  bool still_valid = true;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = index_.find(segments_to_lock.front().lower);
    for (const auto& seg : segments_to_lock) {
      if (it == index_.end() || it->first != seg.lower ||
          !(it->second == seg.sinfo)) {
        still_valid = false;
        break;
      }
      ++it;
    }
  }

  // Caller will need to retry; the segment ranges have changed.
  if (!still_valid) {
    assert(num_granted == segments_to_lock.size());
    for (const auto& seg : segments_to_lock) {
      lock_manager_->ReleaseSegmentLock(seg.sinfo.id(),
                                        LockManager::SegmentMode::kReorg);
    }
    return false;
  }

  return true;
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

uint64_t SegmentIndex::GetSizeFootprint() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return bytes_allocated_ + sizeof(*this);
}

uint64_t SegmentIndex::GetNumEntries() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return index_.size();
}

}  // namespace pg
}  // namespace tl
