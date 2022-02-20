#pragma once

#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>

#include "llsm/pg_db.h"
#include "lock_manager.h"
#include "segment_info.h"
#include "tlx/btree_map.h"

namespace llsm {
namespace pg {

// Maps keys to segments.
class SegmentIndex {
 public:
  // Base key and segment metadata.
  using Entry = std::pair<Key, SegmentInfo>;

  explicit SegmentIndex(std::shared_ptr<LockManager> lock_manager);

  // Used for initializing the segment index.
  template <typename Iterator>
  void BulkLoadFromEmpty(Iterator begin, Iterator end) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    index_.bulk_load(begin, end);
  }

  // Atomically retrieves the segment that is responsible for `key` and acquires
  // a lock on the segment. This method is only meant to be used for acquiring
  // locks with the `kPageRead` and `kPageWrite` modes.
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

  Entry SegmentForKey(const Key key) const;
  std::optional<Entry> NextSegmentForKey(const Key key) const;
  std::vector<Entry> FindRewriteRegion(const Key segment_base) const;

  void SetSegmentOverflow(const Key key, bool overflow);

  // Run `c` while holding an exclusive latch on the index.
  template <typename Callable>
  void RunExclusive(const Callable& c) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    c(index_);
  }

  // Not intended for external use (used by the tests). Not thread safe.
  auto BeginIterator() const { return index_.begin(); }
  auto EndIterator() const { return index_.end(); }

 private:
  Entry& SegmentForKeyImpl(const Key key);
  const Entry& SegmentForKeyImpl(const Key key) const;

  // Used for acquiring segment and page locks. This pointer never changes after
  // the segment index is constructed.
  std::shared_ptr<LockManager> lock_manager_;

  mutable std::shared_mutex mutex_;
  // TODO: In theory, this can be any ordered key-value data structure (e.g.,
  // ART).
  tlx::btree_map<Key, SegmentInfo> index_;
};

}  // namespace pg
}  // namespace llsm
