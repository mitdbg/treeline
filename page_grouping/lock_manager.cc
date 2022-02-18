#include "lock_manager.h"

namespace llsm {
namespace pg {

bool LockManager::TryAcquireSegmentLock(SegmentId seg_id, SegmentMode mode) {
  return false;
}

void LockManager::ReleaseSegmentLock(SegmentId seg_id, SegmentMode mode) {}

bool LockManager::TryAcquirePageLock(SegmentId seg_id, size_t page_idx,
                                     PageMode mode) {
  return false;
}

void LockManager::ReleasePageLock(SegmentId seg_id, size_t page_idx,
                                  PageMode mode) {}

}  // namespace pg
}  // namespace llsm
