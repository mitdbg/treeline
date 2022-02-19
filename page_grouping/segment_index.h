#pragma once

#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>

#include "llsm/pg_db.h"
#include "segment_info.h"
#include "tlx/btree_map.h"

namespace llsm {
namespace pg {

// Maps keys to segments.
class SegmentIndex {
 public:
  // Base key and segment metadata.
  using Entry = std::pair<Key, SegmentInfo>;

  // Used for initializing the segment index.
  template <typename Iterator>
  void BulkLoadFromEmpty(Iterator begin, Iterator end) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    index_.bulk_load(begin, end);
  }

  Entry SegmentForKey(const Key key) const;
  std::optional<Entry> NextSegmentForKey(const Key key) const;

  void SetSegmentOverflow(const Key key, bool overflow);

  // Not intended for external use (used by the tests). Not thread safe.
  auto BeginIterator() const { return index_.begin(); }
  auto EndIterator() const { return index_.end(); }

 private:
  Entry& SegmentForKeyImpl(const Key key);
  const Entry& SegmentForKeyImpl(const Key key) const;

  mutable std::shared_mutex mutex_;
  // TODO: In theory, this can be any ordered key-value data structure (e.g.,
  // ART).
  tlx::btree_map<Key, SegmentInfo> index_;
};

}  // namespace pg
}  // namespace llsm
