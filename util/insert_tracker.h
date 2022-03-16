#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace llsm {

// TODO thread safe access

// Size of reservoir sample.
constexpr size_t kSampleSize = 10000;

class InsertTracker {
 public:
  explicit InsertTracker(const size_t num_inserts_per_epoch,
                         const size_t num_partitions)
      : num_inserts_per_epoch_(num_inserts_per_epoch),
        num_partitions_(num_partitions),
        num_inserts_curr_epoch_(0),
        last_epoch_is_valid_(false) {}

  // Forbid copying and moving.
  InsertTracker(const InsertTracker&) = delete;
  InsertTracker& operator=(const InsertTracker&) = delete;
  InsertTracker(InsertTracker&&) = delete;
  InsertTracker& operator=(InsertTracker&&) = delete;

  void Add(const uint64_t key) {
    if (reservoir_sample_.size() < kSampleSize) {
      // We are still in the warm-up phase. Fill up the sample.
      reservoir_sample_.push_back(key);

      if (reservoir_sample_.size() == kSampleSize) {
        InitializeCurrEpoch();
      }

      return;
    }

    // Reservoir sample is full and boundaries are set.

    ++num_inserts_curr_epoch_;
    AddKeyToSample(key);
    AddKeyToCurrEpoch(key);

    if (num_inserts_curr_epoch_ == num_inserts_per_epoch_) {
      // "Freeze" current epoch and start a new epoch.
      partition_counters_last_epoch_.swap(partition_counters_curr_epoch_);
      partition_boundaries_last_epoch_.swap(partition_boundaries_curr_epoch_);
      InitializeCurrEpoch();

      num_inserts_curr_epoch_ = 0;
      // From now on one can query the last epoch.
      last_epoch_is_valid_ = true;
    }
  }

  // Extrapolates inserts during the last epoch to `num_future_epochs` future
  // epochs. `range_end` is inclusive. Returns false if the last epoch hasn't
  // been initialized yet.
  bool GetNumInsertsInKeyRangeForNumFutureEpochs(
      const uint64_t range_start, const uint64_t range_end,
      const size_t num_future_epochs, size_t* num_inserts_future_epochs) const {
    size_t num_inserts_last_epoch;
    if (!GetNumInsertsInLastEpoch(range_start, range_end,
                                  &num_inserts_last_epoch)) {
      return false;
    }
    *num_inserts_future_epochs = num_inserts_last_epoch * num_future_epochs;
  }

 private:
  // See Algorithm L: https://en.wikipedia.org/wiki/Reservoir_sampling
  void AddKeyToSample(const uint64_t key) {
    // TODO
  }

  void AddKeyToCurrEpoch(const uint64_t key) {
    // Find partition and increase counter.
    const auto it =
        std::upper_bound(partition_boundaries_curr_epoch_.begin(),
                         partition_boundaries_curr_epoch_.end(), key);

    if (it == partition_boundaries_curr_epoch_.begin()) {
      // `key` is out of range (first boundary is larger than `key`).
      return;
    }

    if (it == partition_boundaries_curr_epoch_.end()) {
      // `key` is out of range (all boundaries are smaller than `key`). We could
      // also add the key to the last partition in that case...
      return;
    }

    // `index` marks the upper boundary, the key actually belongs to the
    // partition at `index - 1` as the upper boundary is exclusive.
    const size_t index =
        std::distance(partition_boundaries_curr_epoch_.begin(), it);
    ++partition_counters_curr_epoch_[index - 1];
  }

  // Starts a new epoch and sets equi-depth partition boundaries according to
  // the current sample.
  void InitializeCurrEpoch() {
    partition_counters_curr_epoch_.resize(num_partitions_);
    partition_boundaries_curr_epoch_.resize(num_partitions_ + 1);
    std::fill(partition_counters_curr_epoch_.begin(),
              partition_counters_curr_epoch_.end(), 0);

    // Create a sorted copy of the sample.
    std::vector<uint64_t> sorted_sample(reservoir_sample_);
    std::sort(sorted_sample.begin(), sorted_sample.end());

    const size_t num_records_per_partition =
        sorted_sample.size() / num_partitions_;

    for (int i = 0; i < num_partitions_; ++i) {
      const uint64_t start_key = sorted_sample[i * num_records_per_partition];
      partition_boundaries_curr_epoch_[i] = start_key;
    }

    // Add an extra key at the end (upper boundary of last partition).
    partition_boundaries_curr_epoch_[num_partitions_] =
        sorted_sample[sorted_sample.size() - 1];

    // TODO should we make the last partition correspond to the sample (and use
    // the max key in the sample as upper boundary) or use uint64_t::max
    // instead? The latter would better capture append-only settings => all
    // inserts would go into the last partition. We would need to make sure that
    // append-only inserts actually go into the last page...
  }

  bool GetNumInsertsInLastEpoch(const uint64_t range_start,
                                const uint64_t range_end,
                                size_t* num_inserts_last_epoch) const {
    if (!last_epoch_is_valid_) {
      // Last epoch hasn't been initialized.
      return false;
    }
    *num_inserts_last_epoch = 0;
    for (int i = 0; i < partition_boundaries_last_epoch_.size(); ++i) {
      if (range_start < partition_boundaries_last_epoch_[i + 1] &&
          range_end >= partition_boundaries_last_epoch_[i]) {
        // TODO interpolate within partition (e.g., if 50% of a partition
        // overlaps)
        num_inserts_last_epoch += partition_counters_last_epoch_[i];
      }
    }
  }

  // The maximum number of inserts per epoch. Once that number has been reached,
  // we will start a new epoch.
  size_t num_inserts_per_epoch_;
  // Number of equi-depth partitions according to the sample.
  size_t num_partitions_;
  // The current number of inserts, will be incremented with each insert.
  size_t num_inserts_curr_epoch_;

  // Whether we have observed at least one epoch, i.e., the last epoch is valid.
  bool last_epoch_is_valid_;

  std::vector<uint64_t> reservoir_sample_;

  std::vector<size_t> partition_counters_curr_epoch_;
  std::vector<uint64_t> partition_boundaries_curr_epoch_;

  std::vector<size_t> partition_counters_last_epoch_;
  std::vector<uint64_t> partition_boundaries_last_epoch_;
};

}  // namespace llsm
