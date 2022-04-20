#include <future>
#include <thread>

#include "gtest/gtest.h"
#include "page_grouping/lock_manager.h"

namespace {

using namespace tl;
using namespace tl::pg;

TEST(PGLockManagerTest, PageLockCompatibility) {
  LockManager m;
  const SegmentId sid(0, 0);
  const size_t page_idx = 0;

  // Shared is compatible with shared but not exclusive.
  ASSERT_TRUE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kShared));
  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kExclusive));
  ASSERT_TRUE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kShared));

  m.ReleasePageLock(sid, page_idx, LockManager::PageMode::kShared);
  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kExclusive));
  m.ReleasePageLock(sid, page_idx, LockManager::PageMode::kShared);

  // Exclusive is not compatible with any mode.
  ASSERT_TRUE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kExclusive));
  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kShared));
  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page_idx, LockManager::PageMode::kExclusive));

  m.ReleasePageLock(sid, page_idx, LockManager::PageMode::kExclusive);
}

TEST(PGLockManagerTest, PageLockDistinct) {
  LockManager m;
  const SegmentId sid(0, 0);
  const size_t page1 = 0, page2 = 1;

  // Locks for distinct pages should not affect each other.
  ASSERT_TRUE(
      m.TryAcquirePageLock(sid, page1, LockManager::PageMode::kExclusive));
  ASSERT_TRUE(m.TryAcquirePageLock(sid, page2, LockManager::PageMode::kShared));

  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page1, LockManager::PageMode::kExclusive));
  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page2, LockManager::PageMode::kExclusive));

  ASSERT_FALSE(
      m.TryAcquirePageLock(sid, page1, LockManager::PageMode::kShared));
  ASSERT_TRUE(m.TryAcquirePageLock(sid, page2, LockManager::PageMode::kShared));

  m.ReleasePageLock(sid, page2, LockManager::PageMode::kShared);
  m.ReleasePageLock(sid, page2, LockManager::PageMode::kShared);
  m.ReleasePageLock(sid, page1, LockManager::PageMode::kExclusive);
}

TEST(PGLockManagerTest, SegmentLockCompatibility) {
  LockManager m;
  const SegmentId sid(0, 0);

  // IR and IW are compatible.
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));

  // O is not compatible with IW.
  ASSERT_FALSE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageWrite);
  ASSERT_FALSE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageWrite);
  // But O is compatible with IR.
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));

  // Release all locks.
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kReorg);

  // Test compatibility when reorg is acquired first.
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));

  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kReorg);
}

TEST(PGLockManagerTest, SegmentLockReorgUpgrade) {
  // Tests the reorg to reorg exclusive upgrade flow.
  // Note: Incorrect behavior in the `LockManager` may cause the main thread
  // running this test to enter a synchronization deadlock (waiting for an event
  // that will never happen).

  LockManager m;
  const SegmentId sid(0, 0);

  // One thread is reading pages.
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));

  std::promise<void> reorg_started, done_acquiring, upgrade_complete,
      done_asserting;

  // Another thread runs reorg and then upgrades.
  std::thread reorg_thread([&m, &sid, &reorg_started,
                            done_acquiring = done_acquiring.get_future(),
                            &upgrade_complete,
                            done_asserting =
                                done_asserting.get_future()]() mutable {
    ASSERT_TRUE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
    reorg_started.set_value();
    done_acquiring.get();
    m.UpgradeSegmentLockToReorgExclusive(sid);
    upgrade_complete.set_value();
    done_asserting.get();
    m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kReorgExclusive);
  });

  reorg_started.get_future().get();

  // Lock acquired in reorg mode. Additional read acquisitions are allowed, but
  // not other write or reorg acquisitions.
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_FALSE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));

  // Have the "reorg" thread proceed with upgrading its lock.
  done_acquiring.set_value();

  // Release our previously acquired read locks. This should allow the upgrade
  // to reorg exclusive to complete.
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);

  // Wait for the upgrade to finish.
  upgrade_complete.get_future().get();

  // No other lock mode acquisitions are allowed. (None are allowed as soon as
  // an upgrade is initiated, but it's difficult to test for that accurately due
  // to nondeterministic thread interleavings).
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));
  ASSERT_FALSE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  done_asserting.set_value();

  // Wait for the thread to finish.
  reorg_thread.join();

  // Page read/write should be allowed again.
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageWrite));
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageWrite);
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kReorg));
  ASSERT_TRUE(
      m.TryAcquireSegmentLock(sid, LockManager::SegmentMode::kPageRead));
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid, LockManager::SegmentMode::kReorg);
}

TEST(PGLockManagerTest, SegmentLockDistinct) {
  LockManager m;
  const SegmentId sid1(0, 0), sid2(0, 16);

  // Locks for distinct segments should not affect each other.
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid1, LockManager::SegmentMode::kReorg));
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid2, LockManager::SegmentMode::kPageWrite));

  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid1, LockManager::SegmentMode::kPageWrite));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid1, LockManager::SegmentMode::kReorg));
  ASSERT_FALSE(
      m.TryAcquireSegmentLock(sid2, LockManager::SegmentMode::kReorg));

  ASSERT_TRUE(m.TryAcquireSegmentLock(sid1, LockManager::SegmentMode::kPageRead));
  ASSERT_TRUE(m.TryAcquireSegmentLock(sid2, LockManager::SegmentMode::kPageRead));

  m.ReleaseSegmentLock(sid1, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid1, LockManager::SegmentMode::kReorg);
  m.ReleaseSegmentLock(sid2, LockManager::SegmentMode::kPageRead);
  m.ReleaseSegmentLock(sid2, LockManager::SegmentMode::kPageWrite);
}

}  // namespace
