#pragma once

#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>

#include "treeline/pg_db.h"
#include "lock_manager.h"
#include "segment_info.h"
#include "third_party/tlx/btree_map.h"
#include "util/tracking_allocator.h"

namespace tl {
namespace pg {

// Maps keys to segments.
class SegmentIndex {
 public:
  struct Entry {
    // The key boundaries of the segment.
    // Lower is inclusive (and is the segment's base key). Upper is exclusive.
    Key lower, upper;
    SegmentInfo sinfo;
  };

  explicit SegmentIndex(std::shared_ptr<LockManager> lock_manager);

  // Used for initializing the segment index.
  template <typename Iterator>
  void BulkLoadFromEmpty(Iterator begin, Iterator end) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    index_.bulk_load(begin, end);
  }

  // Atomically retrieves the segment that is responsible for `key` and acquires
  // a lock on the segment. This method is only meant to be used for acquiring
  // locks with the `kPageRead`, `kPageWrite`, and `kReorg` modes.
  //
  // The caller is responsible for releasing the lock.
  Entry SegmentForKeyWithLock(const Key key,
                              LockManager::SegmentMode mode) const;

  // Atomically retrieves the segment that logically follows the segment that is
  // responsible for `key` and acquires a lock on that segment if it exists.
  // This method is only meant to be used for acquiring locks with the
  // `kPageRead` and `kPageWrite` modes.
  //
  // If the returned optional is non-empty, then the caller will hold a lock on
  // the segment in the requested mode. The caller is responsible for later
  // releasing the lock.
  //
  // If the returned optional is empty, it indicates that there is no "logically
  // next" segment (i.e., the segment that holds `key` is the last segment in
  // the DB).
  std::optional<Entry> NextSegmentForKeyWithLock(
      const Key key, LockManager::SegmentMode mode) const;

  // Returns the boundaries of the segment on which `key` should be stored. The
  // lower bound is inclusive and the upper bound is exclusive.
  std::pair<Key, Key> GetSegmentBoundsFor(const Key key) const;

  // Similar to the "WithLock" versions, but does not acquire any segment locks.
  Entry SegmentForKey(const Key key) const;
  std::optional<Entry> NextSegmentForKey(const Key key) const;

  // Find a contiguous segment range to rewrite and acquire locks in `kReorg`
  // mode on the segments. If the returned vector is empty, the caller must
  // retry the call.
  std::vector<Entry> FindAndLockRewriteRegion(const Key segment_base,
                                              uint32_t search_radius) const;

  // Find a contiguous segment range where each segment contains at least one
  // overflow page and acquire locks in `kReorg` mode on the segments. This
  // method starts its search from `start_key` and stops its search before
  // `end_key` (exclusive upper bound).
  //
  // If the returned optional is empty, the caller must retry the call. If the
  // returned optional is non-empty but the vector is empty, it means there are
  // no overflow pages in the segments starting at and following `start_key`.
  std::optional<std::vector<Entry>> FindAndLockNextOverflowRegion(
      const Key start_key, const Key end_key) const;

  // Mark whether or not the segment storing `key` has an overflow page.
  void SetSegmentOverflow(const Key key, bool overflow);

  // Run `c` while holding an exclusive latch on the index.
  template <typename Callable>
  void RunExclusive(const Callable& c) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    c(index_);
  }

  uint64_t GetSizeFootprint() const;
  uint64_t GetNumEntries() const;

  // Not intended for external use (used by the tests). Not thread safe.
  auto BeginIterator() const { return index_.begin(); }
  auto EndIterator() const { return index_.end(); }

 private:
  using OrderedMap = tlx::btree_map<
      Key, SegmentInfo, std::less<Key>,
      tlx::btree_default_traits<Key, std::pair<Key, SegmentInfo>>,
      TrackingAllocator<std::pair<Key, SegmentInfo>>>;

  // Returns true iff all the lock acquisitions succeed. `segments_to_lock` must
  // be sorted by the segments' lower bounds.
  bool LockSegmentsForRewrite(
      const std::vector<SegmentIndex::Entry>& segments_to_lock) const;
  OrderedMap::iterator SegmentForKeyImpl(const Key key);
  OrderedMap::const_iterator SegmentForKeyImpl(const Key key) const;
  Entry IndexIteratorToEntry(OrderedMap::const_iterator it) const;

  // Used for acquiring segment locks. This pointer never changes after the
  // segment index is constructed.
  std::shared_ptr<LockManager> lock_manager_;

  mutable std::shared_mutex mutex_;
  uint64_t bytes_allocated_;
  // Note: In theory, this can be any ordered key-value data structure (e.g.,
  // ART).
  OrderedMap index_;
};

}  // namespace pg
}  // namespace tl
