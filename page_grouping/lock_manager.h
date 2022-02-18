#pragma once
#include <string>

#include "libcuckoo/cuckoohash_map.hh"
#include "persist/segment_id.h"

namespace llsm {
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
  enum class PageMode : uint8_t {
    kShared = 0,    // S
    kExclusive = 1  // X
  };

  // Try to acquire a segment lock on `seg_id` with mode `mode`. Returns true
  // iff the acquisiton was successful.
  //
  // If the acquisition was successful, the caller is responsible for calling
  // `ReleaseSegmentLock()` with the same mode to release the lock.
  bool TryAcquireSegmentLock(SegmentId seg_id, SegmentMode mode);

  // Release a segment lock that was previously acquired on `seg_id` with mode
  // `mode`.
  void ReleaseSegmentLock(SegmentId seg_id, SegmentMode mode);

  // Try to acquire a page lock on `seg_id` and page index `page_idx` with mode
  // `mode`. Returns true iff the acquisiton was successful.
  //
  // If the acquisition was successful, the caller is responsible for calling
  // `ReleasePageLock()` with the same mode to release the lock.
  bool TryAcquirePageLock(SegmentId seg_id, size_t page_idx, PageMode mode);

  // Release a page lock that was previously acquired on `seg_id` and page index
  // `page_idx` with mode `mode`.
  void ReleasePageLock(SegmentId seg_id, size_t page_idx, PageMode mode);

 private:
  struct SegmentLockState {
    uint16_t num_shared = 0;
    uint16_t num_exclusive = 0;
  };
  struct PageLockState {
    uint16_t num_page_read = 0;
    uint16_t num_page_write = 0;
    uint16_t num_reorg = 0;
    uint16_t num_reorg_exclusive = 0;
  };

  libcuckoo::cuckoohash_map<size_t, SegmentLockState> segment_locks_;
  libcuckoo::cuckoohash_map<size_t, PageLockState> page_locks_;
};

}  // namespace pg
}  // namespace llsm
