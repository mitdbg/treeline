#pragma once

#include <atomic>
#include <cstddef>
#include <iostream>

namespace tl {

class Statistics {
 public:
  Statistics() { ClearAll(); }

  Statistics(const Statistics&) = delete;
  Statistics& operator=(const Statistics&) = delete;

  void ClearAll() {
    total_flush_bufmgr_hits_pages_ = 0;
    total_flush_bufmgr_misses_pages_ = 0;
    total_flush_deferred_pages_ = 0;
    total_flush_deferred_records_ = 0;

    total_user_writes_records_ = 0;
    total_user_reads_cache_hits_records_ = 0;
    total_user_reads_bufmgr_hits_records_ = 0;
    total_user_reads_single_bufmgr_misses_records_ = 0;
    total_user_reads_multi_bufmgr_misses_records_ = 0;

    ClearTemp();

    reorg_count_ = 0;
    reorg_pre_total_length_ = 0;
    reorg_post_total_length_ = 0;

    preorg_count_ = 0;
    preorg_pre_total_length_ = 0;
    preorg_post_total_length_ = 0;
  }

  void ClearTemp() {
    temp_flush_bufmgr_hits_pages_ = 0;
    temp_flush_bufmgr_misses_pages_ = 0;
    temp_flush_deferred_pages_ = 0;
    temp_flush_deferred_records_ = 0;

    temp_user_writes_records_ = 0;
    temp_user_reads_cache_hits_records_ = 0;
    temp_user_reads_bufmgr_hits_records_ = 0;
    temp_user_reads_single_bufmgr_misses_records_ = 0;
    temp_user_reads_multi_bufmgr_misses_records_ = 0;
  }

  void MoveTempToTotalAndClearTemp() {
    // This is only "atomic" because we know it is ONLY called within
    // DBImpl::ScheduleMemTableFlush() (which owns locks) and DBImpl::~DBImpl.
    // Not necessarily safe to call it elsewhere.
    total_flush_bufmgr_hits_pages_ += temp_flush_bufmgr_hits_pages_;
    total_flush_bufmgr_misses_pages_ += temp_flush_bufmgr_misses_pages_;
    total_flush_deferred_pages_ += temp_flush_deferred_pages_;
    total_flush_deferred_records_ += temp_flush_deferred_records_;

    total_user_writes_records_ += temp_user_writes_records_;
    total_user_reads_cache_hits_records_ +=
        temp_user_reads_cache_hits_records_;
    total_user_reads_bufmgr_hits_records_ +=
        temp_user_reads_bufmgr_hits_records_;
    total_user_reads_single_bufmgr_misses_records_ +=
        temp_user_reads_single_bufmgr_misses_records_;
    total_user_reads_multi_bufmgr_misses_records_ +=
        temp_user_reads_multi_bufmgr_misses_records_;

    ClearTemp();
  }

  std::string to_string() const {
    std::string str;
    str += "Statistics:\n";
    str += "\tIO:\n";
    str +=
        "flush bufmgr hits (pgs), flush bufmgr misses (pgs), flush deferrals "
        "(pgs), flush deferrals (recs), user writes (recs), user reads "
        "cache hits (recs), user reads bufmgr hits (recs), user reads "
        "single bufmgr miss (recs), user reads multiple bufmgr misses (recs)\n";
    str += std::to_string(total_flush_bufmgr_hits_pages_) + "," +
           std::to_string(total_flush_bufmgr_misses_pages_) + "," +
           std::to_string(total_flush_deferred_pages_) + "," +
           std::to_string(total_flush_deferred_records_) + "," +
           std::to_string(total_user_writes_records_) + "," +
           std::to_string(total_user_reads_cache_hits_records_) + "," +
           std::to_string(total_user_reads_bufmgr_hits_records_) + "," +
           std::to_string(total_user_reads_single_bufmgr_misses_records_) +
           "," + std::to_string(total_user_reads_multi_bufmgr_misses_records_) +
           "\n";

    str += "\t(P)reorganization:\n";
    str +=
        "reorganizations, cumulative chain length before reorganization (pgs), "
        "cumulative chain length after reorganization (pgs), preorganizations, "
        "cumulative chain length before preorganization (pgs), cumulative "
        "chain length after preorganization (pgs)\n";
    str += std::to_string(reorg_count_) + "," +
           std::to_string(reorg_pre_total_length_) + "," +
           std::to_string(reorg_post_total_length_) + "," +
           std::to_string(preorg_count_) + "," +
           std::to_string(preorg_pre_total_length_) + "," +
           std::to_string(preorg_post_total_length_) + "\n";

    return str;
  }

  std::atomic<size_t> total_flush_bufmgr_hits_pages_;
  std::atomic<size_t> total_flush_bufmgr_misses_pages_;
  std::atomic<size_t> total_flush_deferred_pages_;
  std::atomic<size_t> total_flush_deferred_records_;

  std::atomic<size_t> total_user_writes_records_;
  std::atomic<size_t> total_user_reads_cache_hits_records_;
  std::atomic<size_t> total_user_reads_bufmgr_hits_records_;
  std::atomic<size_t> total_user_reads_single_bufmgr_misses_records_;
  std::atomic<size_t> total_user_reads_multi_bufmgr_misses_records_;

  std::atomic<size_t> temp_flush_bufmgr_hits_pages_;
  std::atomic<size_t> temp_flush_bufmgr_misses_pages_;
  std::atomic<size_t> temp_flush_deferred_pages_;
  std::atomic<size_t> temp_flush_deferred_records_;

  std::atomic<size_t> temp_user_writes_records_;
  std::atomic<size_t> temp_user_reads_cache_hits_records_;
  std::atomic<size_t> temp_user_reads_bufmgr_hits_records_;
  std::atomic<size_t> temp_user_reads_single_bufmgr_misses_records_;
  std::atomic<size_t> temp_user_reads_multi_bufmgr_misses_records_;

  std::atomic<size_t> reorg_count_;
  std::atomic<size_t> reorg_pre_total_length_;
  std::atomic<size_t> reorg_post_total_length_;

  std::atomic<size_t> preorg_count_;
  std::atomic<size_t> preorg_pre_total_length_;
  std::atomic<size_t> preorg_post_total_length_;
};

}  // namespace tl
