#pragma once

#include <cstdint>

#include "libcuckoo/cuckoohash_map.hh"
#include "persist/segment_id.h"

namespace tl {
namespace pg {

// Used to coordinate concurrent access to segments and pages. The locks granted
// by this manager are logical locks.
//
// Threads are responsible for acquiring and releasing locks consistently (e.g.,
// if you are granted an `kShared` page lock, you must eventually call
// `ReleasePageLock()` with `kShared` as the mode).
//
// This class's methods are safe to call concurrently.
class LockManager {
 public:
  // Segment locking modes.
  // Lock compatibility table
  //     | IR | IW | O  | OX |
  // ----+----+----+----+----+
  //  IR | Y  | Y  | Y  | N  |
  // ----+----+----+----+----+
  //  IW | Y  | Y  | N  | N  |
  // ----+----+----+----+----+
  //  O  | Y  | N  | N  | N  |
  // ----+----+----+----+----+
  //  OX | N  | N  | N  | N  |
  // ----+----+----+----+----+
  enum class SegmentMode : uint8_t {
    kPageRead = 0,       // IR
    kPageWrite = 1,      // IW
    kReorg = 2,          // O
    kReorgExclusive = 3  // OX
  };

  // Page locking modes.
  // Lock compatibility table
  //    | S | X |
  // ---+---+---+
  //  S | Y | N |
  // ---+---+---+
  //  X | N | N |
  // ---+---+---+
  // Page locks are currently only acquired on the "main" page located in a
  // segment. As a result, the page lock on the main page protects both the main
  // page and its overflow (if one exists).
  enum class PageMode : uint8_t {
    kShared = 0,    // S
    kExclusive = 1  // X
  };

  // Try to acquire a segment lock on `seg_id` with mode `mode`. Returns true
  // iff the acquisiton was successful.
  //
  // If the acquisition was successful, the caller is responsible for calling
  // `ReleaseSegmentLock()` with the same mode to release the lock.
  bool TryAcquireSegmentLock(const SegmentId& seg_id, SegmentMode mode);

  // Release a segment lock that was previously acquired on `seg_id` with mode
  // `mode`.
  void ReleaseSegmentLock(const SegmentId& seg_id, SegmentMode mode);

  // Upgrade the segment lock on `seg_id` to `kReorgExclusive`. The caller must
  // already hold the lock in `kReorg` mode. This method will block until the
  // upgraded lock can be granted (it will wait for any concurrent readers to
  // release their `kPageRead` lock(s)).
  void UpgradeSegmentLockToReorgExclusive(const SegmentId& seg_id);

  // Try to acquire a page lock on `seg_id` and page index `page_idx` with mode
  // `mode`. Returns true iff the acquisiton was successful.
  //
  // If the acquisition was successful, the caller is responsible for calling
  // `ReleasePageLock()` with the same mode to release the lock.
  bool TryAcquirePageLock(const SegmentId& seg_id, size_t page_idx, PageMode mode);

  // Acquire a page lock on `seg_id` and page index `page_idx` with mode `mode`.
  // After returning, the caller will hold the lock. The caller is responsible
  // for calling `ReleasePageLock()` with the same mode to release the lock.
  void AcquirePageLock(const SegmentId& seg_id, size_t page_idx, PageMode mode);

  // Release a page lock that was previously acquired on `seg_id` and page index
  // `page_idx` with mode `mode`.
  void ReleasePageLock(const SegmentId& seg_id, size_t page_idx, PageMode mode);

 private:
  using LockCount = uint16_t;
  using LockId = size_t;

  struct SegmentLockState {
    explicit SegmentLockState(SegmentMode initial_mode);
    LockCount num_page_read = 0;
    LockCount num_page_write = 0;
    LockCount num_reorg = 0;
    LockCount num_reorg_exclusive = 0;
  };
  struct PageLockState {
    explicit PageLockState(PageMode initial_mode);
    LockCount num_shared = 0;
    LockCount num_exclusive = 0;
  };

  LockId SegmentLockId(const SegmentId& seg_id) const;
  LockId PageLockId(const SegmentId& seg_id, size_t page_idx) const;

  libcuckoo::cuckoohash_map<LockId, SegmentLockState> segment_locks_;
  libcuckoo::cuckoohash_map<LockId, PageLockState> page_locks_;
};

}  // namespace pg
}  // namespace tl
