#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <mutex>
#include <random>
#include <vector>

namespace tl {

// Tracks the distribution of an (insert) workload using an equi-depth
// histogram. The histogram boundaries are set using a sample which is
// maintained using reservoir sampling. The inserts are tracked per "epoch"
// which is defined as a certain number of inserts. Queries always return the
// statistics of the last completed epoch (if such an epoch exists).
class InsertTracker {
 public:
  InsertTracker(const size_t num_inserts_per_epoch, const size_t num_partitions,
                const size_t sample_size, const size_t random_seed = 42)
      : num_inserts_per_epoch_(num_inserts_per_epoch),
        num_partitions_(num_partitions),
        num_inserts_(0),
        num_inserts_curr_epoch_(0),
        sample_size_(sample_size),
        last_epoch_is_valid_(false),
        next_(sample_size_ + 1),
        gen_(random_seed) {
    std::uniform_real_distribution<double> real_dist(0.0, 1.0);
    w_ = exp(log(real_dist(gen_)) / sample_size_);
  }

  // Forbid copying and moving.
  InsertTracker(const InsertTracker&) = delete;
  InsertTracker& operator=(const InsertTracker&) = delete;
  InsertTracker(InsertTracker&&) = delete;
  InsertTracker& operator=(InsertTracker&&) = delete;

  // Tracks an insert. Should be called for each individual insert.
  void Add(const uint64_t key) {
    const std::lock_guard<std::mutex> lock(mutex_);

    ++num_inserts_;

    if (reservoir_sample_.size() < sample_size_) {
      // Fill up the sample.
      reservoir_sample_.push_back(key);

      if (reservoir_sample_.size() == sample_size_) {
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
  // epochs. `range_end` is exclusive. Returns false if the last epoch hasn't
  // been initialized yet.
  bool GetNumInsertsInKeyRangeForNumFutureEpochs(
      const uint64_t range_start, const uint64_t range_end,
      const size_t num_future_epochs, double* num_inserts_future_epochs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    double num_inserts_last_epoch;
    if (!GetNumInsertsInLastEpoch(range_start, range_end,
                                  &num_inserts_last_epoch)) {
      return false;
    }
    *num_inserts_future_epochs = num_inserts_last_epoch * num_future_epochs;
    return true;
  }

 private:
  // See Algorithm L: https://en.wikipedia.org/wiki/Reservoir_sampling
  void AddKeyToSample(const uint64_t key) {
    if (num_inserts_ == next_) {
      // Replace random item with `key`.
      std::uniform_int_distribution<size_t> int_dist(0, sample_size_ - 1);
      reservoir_sample_[int_dist(gen_)] = key;

      // Update `next_` and `w_`.
      std::uniform_real_distribution<double> real_dist(0.0, 1.0);
      next_ += static_cast<size_t>(log(real_dist(gen_)) / (log(1.0 - w_))) + 1;
      w_ *= exp(log(real_dist(gen_)) / sample_size_);
    }
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
      // `key` is out of range (all boundaries are smaller than `key`). Can't
      // happen since the bonudary of the last partition is uint64_t::max.
      std::cerr << "Reached unreachable code" << std::endl;
      assert(false);
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

    // Add an extra key at the end (uint64_t::max).
    partition_boundaries_curr_epoch_[num_partitions_] =
        std::numeric_limits<uint64_t>::max();
  }

  bool GetNumInsertsInLastEpoch(const uint64_t range_start,
                                const uint64_t range_end,
                                double* num_inserts_last_epoch) {
    if (!last_epoch_is_valid_) {
      // Last epoch hasn't been initialized.
      return false;
    }
    *num_inserts_last_epoch = 0;
    for (int i = 0; i < num_partitions_; ++i) {
      const uint64_t partition_start = partition_boundaries_last_epoch_[i];
      const uint64_t partition_end = partition_boundaries_last_epoch_[i + 1];

      if (range_start < partition_end && range_end > partition_start) {
        // Interpolate within partition (e.g., if 50% of a partition
        // overlaps).
        const uint64_t partition_range = partition_end - partition_start;
        const uint64_t query_start = std::max(partition_start, range_start);
        const uint64_t query_end = std::min(partition_end, range_end);

        const uint64_t query_range = query_end - query_start;
        const double overlap =
            static_cast<double>(query_range) / partition_range;  // (0,1]

        const double interpolated_inserts =
            partition_counters_last_epoch_[i] * overlap;

        *num_inserts_last_epoch += interpolated_inserts;
      }
    }
    return true;
  }

  // The maximum number of inserts per epoch. Once that number has been
  // reached, we will start a new epoch.
  size_t num_inserts_per_epoch_;
  // Number of equi-depth partitions according to the sample.
  size_t num_partitions_;
  // Total number of inserts.
  size_t num_inserts_;
  // Number of inserts in the current epoch.
  size_t num_inserts_curr_epoch_;
  // Size of the reservoir sample.
  size_t sample_size_;

  // Whether we have observed at least one epoch, i.e., the last epoch is
  // valid.
  bool last_epoch_is_valid_;

  // The following vectors track the number of inserts in the current / last
  // epoch per partition of the equi-depth histogram. The boundary values are
  // the inclusive lower bounds of each partition.
  std::vector<size_t> partition_counters_curr_epoch_;
  std::vector<uint64_t> partition_boundaries_curr_epoch_;
  std::vector<size_t> partition_counters_last_epoch_;
  std::vector<uint64_t> partition_boundaries_last_epoch_;

  // Reservoir sample.
  std::vector<uint64_t> reservoir_sample_;
  double w_;
  size_t next_;
  std::mt19937 gen_;

  // Global mutex.
  std::mutex mutex_;
};

}  // namespace tl
