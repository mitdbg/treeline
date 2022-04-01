#include "util/insert_tracker.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

using tl::InsertTracker;

TEST(InsertTrackerTest, UpToOneCompletedEpoch) {
  const size_t num_inserts = 100;
  const size_t sample_size = 10;
  const size_t num_inserts_per_epoch = num_inserts - sample_size;
  const size_t num_partitions = 1;

  // The first epoch starts after the sample has been filled up (after 10
  // inserts). So the epoch will be completed after 100 inserts.

  InsertTracker tracker(num_inserts_per_epoch, num_partitions, sample_size);

  // Not completed yet.
  double num_inserts_future_epochs;
  ASSERT_FALSE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));

  for (uint64_t i = 0; i < 99; ++i) {
    tracker.Add(i);
  }

  // Not completed yet.
  ASSERT_FALSE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));

  tracker.Add(99);

  // Completed!

  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
}

TEST(InsertTrackerTest, OneCompletedEpochOnePartition) {
  const size_t num_inserts = 100;
  const size_t sample_size = 10;
  const size_t num_inserts_per_epoch = num_inserts - sample_size;
  const size_t num_partitions = 1;

  InsertTracker tracker(num_inserts_per_epoch, num_partitions, sample_size);
  for (uint64_t i = 0; i < num_inserts; ++i) {
    tracker.Add(i);
  }

  double num_inserts_future_epochs;
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 90);
}

TEST(InsertTrackerTest, OneCompletedEpochTwoPartitions) {
  const size_t num_inserts = 100;
  const size_t sample_size = 10;
  const size_t num_inserts_per_epoch = num_inserts - sample_size;
  const size_t num_partitions = 2;

  InsertTracker tracker(num_inserts_per_epoch, num_partitions, sample_size);
  for (uint64_t i = 0; i < num_inserts; ++i) {
    tracker.Add(i % 10);
  }

  double num_inserts_future_epochs;
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, 5, /*num_future_epochs=*/1, &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 45);

  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      5, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 45);
}

TEST(InsertTrackerTest, OneCompletedEpochTwoPartitionsInterpolate) {
  const size_t num_inserts = 100;
  const size_t sample_size = 10;
  const size_t num_inserts_per_epoch = num_inserts - sample_size;
  const size_t num_partitions = 2;

  InsertTracker tracker(num_inserts_per_epoch, num_partitions, sample_size);
  for (uint64_t i = 0; i < num_inserts; ++i) {
    tracker.Add(i % 10);
  }

  // 100% overlap.
  double num_inserts_future_epochs;
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, 5, /*num_future_epochs=*/1, &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 45);

  // 80% overlap.
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, 4, /*num_future_epochs=*/1, &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 36);

  // 20% overlap.
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, 1, /*num_future_epochs=*/1, &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 9);
}

TEST(InsertTrackerTest, TwoCompletedEpochsOnePartition) {
  const size_t num_inserts = 100;
  const size_t sample_size = 10;
  const size_t num_inserts_per_epoch = num_inserts - sample_size;
  const size_t num_partitions = 1;

  InsertTracker tracker(num_inserts_per_epoch, num_partitions, sample_size);
  for (uint64_t i = 0; i < num_inserts; ++i) {
    tracker.Add(i);
  }

  // One completed epoch.
  double num_inserts_future_epochs;
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 90);

  for (uint64_t i = 0; i < num_inserts_per_epoch - 1; ++i) {
    tracker.Add(i);
  }

  // Second epoch hasn't been completed yet. Result is expected to still be the
  // same.
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 90);

  tracker.Add(num_inserts_per_epoch - 1);

  // Now it has completed. At this point, the partition boundaries have been
  // re-set using the sample. Since the smallest key in the sample is 6, the
  // lower boundary of the partition changed from 0 to 6. So we don't track all
  // 90 inserts anymore but in this case only 84.
  ASSERT_TRUE(tracker.GetNumInsertsInKeyRangeForNumFutureEpochs(
      0, std::numeric_limits<uint64_t>::max(), /*num_future_epochs=*/1,
      &num_inserts_future_epochs));
  ASSERT_EQ(num_inserts_future_epochs, 84);
}

// TEST(InsertTrackerTest, Perf) {
//   InsertTracker tracker(/*num_inserts_per_epoch=*/1000,
//   /*num_partitions=*/100,
//                         /*sample_size=*/10000);

//   const size_t num_inserts = 10000000;

//   auto start = std::chrono::steady_clock::now();

//   for (uint64_t i = 0; i < num_inserts; ++i) {
//     tracker.Add(i);
//   }

//   const size_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
//                         std::chrono::steady_clock::now() - start)
//                         .count();
//   std::cout << "ns per insert: " << static_cast<double>(ns) / num_inserts
//             << std::endl;
// }

}  // namespace
