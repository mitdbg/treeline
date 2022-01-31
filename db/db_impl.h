#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <queue>

#include "bufmgr/buffer_manager.h"
#include "db/format.h"
#include "db/memtable.h"
#include "llsm/db.h"
#include "llsm/statistics.h"
#include "model/model.h"
#include "record_cache/record_cache.h"
#include "util/thread_pool.h"
#include "wal/manager.h"

namespace llsm {

class DBImpl : public DB {
 public:
  DBImpl(Options options, std::filesystem::path db_path);
  ~DBImpl() override;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status BulkLoad(
      const WriteOptions& options,
      std::vector<std::pair<const Slice, const Slice>>& records) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value_out) override;
  Status GetRange(const ReadOptions& options, const Slice& start_key,
                  size_t num_records, RecordBatch* results_out) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status FlushRecordCache(const bool disable_deferred_io) override;

  // Must be called exactly once after `DBImpl` is constructed to initialize the
  // database's internal state. Other public `DBImpl` methods can be called
  // after and only if this method returns `Status::OK()`.
  Status Initialize();

  // Gets the number of pages indexed by the model (for debugging).
  size_t GetNumIndexedPages() const;

  using OverflowChain = std::shared_ptr<std::vector<BufferFrame*>>;
  using FlushBatch = std::vector<
      std::tuple<const Slice, const Slice, const format::WriteType>>;

 private:
  Status InitializeNewDB();
  Status InitializeExistingDB();

  // Records writes and deletes in the active `MemTable`. The `value` is ignored
  // if `write_type` is `WriteType::kDelete`.
  // This method is thread safe.
  Status WriteImpl(const WriteOptions& options, const Slice& key,
                   const Slice& value, format::WriteType write_type);

  // Fixes the page chain starting with the page at `page_id`. The returned page
  // frames can optionally be unlocked before returning. Returns `nullptr` if it
  // detected a reorganization while fixing the first chain link.
  OverflowChain FixOverflowChain(PhysicalPageId page_id, bool exclusive,
                                 bool unlock_before_returning);

  // Reorganizes the page chain starting with the page at `page_id` by promoting
  // overflow pages.
  Status ReorganizeOverflowChain(PhysicalPageId page_id,
                                 uint32_t page_fill_pct);

  // Reorganizes the page chain `chain` so as to efficiently insert `records`.
  Status PreorganizeOverflowChain(const FlushBatch& records,
                                  OverflowChain chain, uint32_t page_fill_pct);

  // Fill `old_records` with copies of all the records in `chain`.
  void ExtractOldRecords(OverflowChain chain, RecordBatch* old_records);

  // Merge the records in `old_records` and `records` to create a single
  // FlushBatch with the union of their records in ascending order. Whenever
  // the same key exists in both colections, the value (or deletion marker)
  // in `records` is given precendence.
  void MergeBatches(RecordBatch& old_records, const FlushBatch& records,
                    FlushBatch* merged);

  // Will not be changed after `Initialize()` returns. The objects below are
  // thread safe; no additional mutual exclusion is required.
  Options options_;
  Statistics stats_;
  const std::filesystem::path db_path_;
  std::unique_ptr<BufferManager> buf_mgr_;
  std::unique_ptr<Model> model_;
  std::unique_ptr<ThreadPool> workers_;
  std::unique_ptr<RecordCache> rec_cache_;

  // Remaining database state protected by `mutex_`.
  std::mutex mutex_;

  // Handles reading from and writing to the write-ahead log.
  // REQUIRES: `mutex_` is held when using the manager.
  wal::Manager wal_;
};

}  // namespace llsm
