#pragma once

#include <atomic>
#include <cstddef>
#include <iostream>

namespace llsm {

class Statistics {
 public:
  Statistics() { ClearAll(); }

  Statistics(const Statistics&) = delete;
  Statistics& operator=(const Statistics&) = delete;

  void ClearAll() {
    total_user_writes_records_ = 0;
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
    temp_user_writes_records_ = 0;
    temp_user_reads_bufmgr_hits_records_ = 0;
    temp_user_reads_single_bufmgr_misses_records_ = 0;
    temp_user_reads_multi_bufmgr_misses_records_ = 0;
  }

  void MoveTempToTotalAndClearTemp() {
    total_user_writes_records_ += temp_user_writes_records_;

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
        "user writes (recs), user reads bufmgr hits (recs), user reads "
        "single bufmgr miss (recs), user reads multiple bufmgr misses (recs)\n";
    str += std::to_string(total_user_writes_records_) + "," +
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

  std::atomic<size_t> total_user_writes_records_;
  std::atomic<size_t> total_user_reads_bufmgr_hits_records_;
  std::atomic<size_t> total_user_reads_single_bufmgr_misses_records_;
  std::atomic<size_t> total_user_reads_multi_bufmgr_misses_records_;

  std::atomic<size_t> temp_user_writes_records_;
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

}  // namespace llsm
