#pragma once

#include <atomic>
#include <cstddef>
#include <iostream>

namespace llsm {

class Statistics {
 public:
  Statistics() { Clear(); }

  Statistics(const Statistics&) = delete;
  Statistics& operator=(const Statistics&) = delete;

  void Clear() {
    flush_count_ = 0;
    page_cached_count_ = 0;
    page_io_count_ = 0;
    deferred_count_ = 0;

    flush_records_ = 0;
    page_cached_records_ = 0;
    page_io_records_ = 0;
    deferred_records_ = 0;
  }

  friend std::ostream& operator<<(std::ostream& output,
                                  const Statistics& stats) {
    output << "flushes, fl records, bufmgr hits, bufmgr hits records, "
              "IOs, IOs records, deferrals, deferrals records"
           << std::endl;
    output << stats.flush_count_ << "," << stats.flush_records_ << ","
           << stats.page_cached_count_ << "," << stats.page_cached_records_
           << "," << stats.page_io_count_ << "," << stats.page_io_records_
           << "," << stats.deferred_count_ << "," << stats.deferred_records_
           << std::endl;
    return output;
  }

  std::atomic<size_t> flush_count_;
  std::atomic<size_t> page_cached_count_;
  std::atomic<size_t> page_io_count_;
  std::atomic<size_t> deferred_count_;

  std::atomic<size_t> flush_records_;
  std::atomic<size_t> page_cached_records_;
  std::atomic<size_t> page_io_records_;
  std::atomic<size_t> deferred_records_;
};

}  // namespace llsm
