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
    flush_bufmgr_hits_pages_ = 0;
    flush_bufmgr_misses_pages_ = 0;
    flush_deferred_pages_ = 0;
    flush_deferred_records_ = 0;

    user_writes_records_ = 0;
    user_reads_memtable_hits_records_ = 0;
    user_reads_bufmgr_hits_records_ = 0;
    user_reads_single_bufmgr_misses_records_ = 0;
    user_reads_multi_bufmgr_misses_records_ = 0;
  }

  friend std::ostream& operator<<(std::ostream& output,
                                  const Statistics& stats) {
    output << "flush bufmgr hits (pgs), flush bufmgr misses (pgs), flush "
              "deferrals (pgs), flush deferrals (recs), user writes (recs), "
              "user reads memtable hits (recs), user reads bufmgr hits (recs), "
              "user reads single bufmgr miss (recs), user reads multiple "
              "bufmgr misses (recs)"
           << std::endl;
    output << stats.flush_bufmgr_hits_pages_ << ","
           << stats.flush_bufmgr_misses_pages_ << ","
           << stats.flush_deferred_pages_ << ","
           << stats.flush_deferred_records_ << "," << stats.user_writes_records_
           << "," << stats.user_reads_memtable_hits_records_ << ","
           << stats.user_reads_bufmgr_hits_records_ << ","
           << stats.user_reads_single_bufmgr_misses_records_ << ","
           << stats.user_reads_multi_bufmgr_misses_records_ << "," << std::endl;
    return output;
  }

  std::atomic<size_t> flush_bufmgr_hits_pages_;
  std::atomic<size_t> flush_bufmgr_misses_pages_;
  std::atomic<size_t> flush_deferred_pages_;
  std::atomic<size_t> flush_deferred_records_;

  std::atomic<size_t> user_writes_records_;
  std::atomic<size_t> user_reads_memtable_hits_records_;
  std::atomic<size_t> user_reads_bufmgr_hits_records_;
  std::atomic<size_t> user_reads_single_bufmgr_misses_records_;
  std::atomic<size_t> user_reads_multi_bufmgr_misses_records_;
};

}  // namespace llsm
