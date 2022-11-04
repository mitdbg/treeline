#include "lock_manager.h"

#include <cassert>

#include "rand_exp_backoff.h"

namespace {

constexpr uint32_t kBackoffSaturate = 12;

}

namespace tl {
namespace pg {

bool LockManager::TryAcquireSegmentLock(const SegmentId& seg_id,
                                        SegmentMode requested_mode) {
  const LockId id = SegmentLockId(seg_id);
  bool granted = false;
  const bool inserted_new = segment_locks_.uprase_fn(
      id,
      [&granted, &requested_mode](SegmentLockState& lock_state) {
        switch (requested_mode) {
          case SegmentMode::kPageRead: {
            if (lock_state.num_reorg_exclusive == 0) {
              ++lock_state.num_page_read;
              granted = true;
            }
            break;
          }
          case SegmentMode::kPageWrite: {
            if (lock_state.num_reorg == 0 &&
                lock_state.num_reorg_exclusive == 0) {
              ++lock_state.num_page_write;
              granted = true;
            }
            break;
          }
          case SegmentMode::kReorg: {
            if (lock_state.num_page_write == 0 && lock_state.num_reorg == 0 &&
                lock_state.num_reorg_exclusive == 0) {
              ++lock_state.num_reorg;
              granted = true;
            }
            break;
          }
          case SegmentMode::kReorgExclusive: {
            // Not supposed to acquire OX mode directly.
            assert(false);
            break;
          }
        }
        return false;  // Return false to keep the lock state.
      },
      requested_mode);
  return inserted_new || granted;
}

void LockManager::ReleaseSegmentLock(const SegmentId& seg_id,
                                     SegmentMode granted_mode) {
  const LockId id = SegmentLockId(seg_id);
  const bool lock_state_found = segment_locks_.erase_fn(
      id, [&granted_mode](SegmentLockState& lock_state) {
        switch (granted_mode) {
          case SegmentMode::kPageRead: {
            assert(lock_state.num_page_read > 0);
            --lock_state.num_page_read;
            break;
          }
          case SegmentMode::kPageWrite: {
            assert(lock_state.num_page_write > 0);
            --lock_state.num_page_write;
            break;
          }
          case SegmentMode::kReorg: {
            assert(lock_state.num_reorg > 0);
            --lock_state.num_reorg;
            break;
          }
          case SegmentMode::kReorgExclusive: {
            assert(lock_state.num_reorg_exclusive > 0);
            --lock_state.num_reorg_exclusive;
            break;
          }
        }
        // If this evaluates to true, the lock state will be erased.
        return lock_state.num_page_read == 0 &&
               lock_state.num_page_write == 0 && lock_state.num_reorg == 0 &&
               lock_state.num_reorg_exclusive == 0;
      });
  // Should not attempt to release a lock that was never acquired.
  assert(lock_state_found);
}

void LockManager::UpgradeSegmentLockToReorgExclusive(const SegmentId& seg_id) {
  const LockId id = SegmentLockId(seg_id);
  bool can_return = false;
  const bool lock_state_found =
      segment_locks_.update_fn(id, [&can_return](SegmentLockState& lock_state) {
        assert(lock_state.num_page_write == 0);
        assert(lock_state.num_reorg == 1);
        assert(lock_state.num_reorg_exclusive == 0);
        lock_state.num_reorg = 0;
        lock_state.num_reorg_exclusive = 1;
        // We need to wait until readers are done.
        can_return = (lock_state.num_page_read == 0);
      });
  assert(lock_state_found);

  // Spin wait until the readers are done.
  if (!can_return) {
    RandExpBackoff backoff(kBackoffSaturate);
    while (!can_return) {
      backoff.Wait();
      const bool found = segment_locks_.find_fn(
          id, [&can_return](const SegmentLockState& lock_state) {
            can_return = (lock_state.num_page_read == 0);
          });
      assert(found);
    }
  }
}

bool LockManager::TryAcquirePageLock(const SegmentId& seg_id,
                                     const size_t page_idx,
                                     const PageMode requested_mode) {
  const LockId id = PageLockId(seg_id, page_idx);
  bool granted = false;
  const bool inserted_new = page_locks_.uprase_fn(
      id,
      [&granted, &requested_mode](PageLockState& lock_state) {
        switch (requested_mode) {
          case PageMode::kExclusive: {
            if (lock_state.num_exclusive == 0 && lock_state.num_shared == 0) {
              // This code should never run since we delete lock states when
              // their counters reach 0.
              ++lock_state.num_exclusive;
              granted = true;
            }
            break;
          }
          case PageMode::kShared: {
            if (lock_state.num_exclusive == 0) {
              ++lock_state.num_shared;
              granted = true;
            }
            break;
          }
        }
        return false;  // Return false to keep the lock state.
      },
      requested_mode);
  return inserted_new || granted;
}

void LockManager::AcquirePageLock(const SegmentId& seg_id,
                                  const size_t page_idx,
                                  const PageMode requested_mode) {
  RandExpBackoff backoff(kBackoffSaturate);
  while (true) {
    const bool granted = TryAcquirePageLock(seg_id, page_idx, requested_mode);
    if (granted) return;
    backoff.Wait();
  }
}

void LockManager::ReleasePageLock(const SegmentId& seg_id,
                                  const size_t page_idx,
                                  const PageMode granted_mode) {
  const LockId id = PageLockId(seg_id, page_idx);
  const bool found =
      page_locks_.erase_fn(id, [&granted_mode](PageLockState& lock_state) {
        switch (granted_mode) {
          case PageMode::kExclusive: {
            assert(lock_state.num_shared == 0);
            assert(lock_state.num_exclusive == 1);
            --lock_state.num_exclusive;
            break;
          }
          case PageMode::kShared: {
            assert(lock_state.num_shared > 0);
            assert(lock_state.num_exclusive == 0);
            --lock_state.num_shared;
            break;
          }
        }
        // If this evaluates to true, the lock state will be erased.
        return lock_state.num_shared == 0 && lock_state.num_exclusive == 0;
      });
  assert(found);
}

LockManager::LockId LockManager::SegmentLockId(const SegmentId& seg_id) const {
  return seg_id.value();
}

LockManager::LockId LockManager::PageLockId(const SegmentId& seg_id,
                                            size_t page_idx) const {
  // Implementation detail: The lower bits of the segment ID are the segment's
  // page offset within the file. So adding the page index to this offset
  // produces a unique identifier for the page.
  return seg_id.value() + page_idx;
}

LockManager::SegmentLockState::SegmentLockState(SegmentMode initial_mode) {
  switch (initial_mode) {
    case SegmentMode::kPageRead:
      num_page_read = 1;
      break;
    case SegmentMode::kPageWrite:
      num_page_write = 1;
      break;
    case SegmentMode::kReorg:
      num_reorg = 1;
      break;
    case SegmentMode::kReorgExclusive:
      // Not supposed to acquire OX mode directly.
      assert(false);
      break;
  }
}

LockManager::PageLockState::PageLockState(PageMode initial_mode) {
  switch (initial_mode) {
    case PageMode::kExclusive:
      num_exclusive = 1;
      break;
    case PageMode::kShared:
      num_shared = 1;
      break;
  }
}

}  // namespace pg
}  // namespace tl
