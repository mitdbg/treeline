#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <tuple>
#include <vector>

#include "bufmgr/buffer_manager.h"
#include "db/format.h"
#include "db/memtable.h"
#include "treeline/db.h"
#include "treeline/statistics.h"
#include "model/model.h"
#include "overflow_chain.h"
#include "record_cache/record_cache.h"
#include "util/thread_pool.h"
#include "wal/manager.h"

namespace tl {

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

 private:
  Status InitializeNewDB();
  Status InitializeExistingDB();

  Status GetWithPage(const ReadOptions& options, const Slice& key,
             std::string* value_out, PageBuffer* page_out = nullptr);

  // Records writes and deletes in the active `MemTable`. The `value` is ignored
  // if `write_type` is `WriteType::kDelete`.
  // This method is thread safe.
  Status WriteImpl(const WriteOptions& options, const Slice& key,
                   const Slice& value, format::WriteType write_type);

  // Writes the records in the batch to persistent storage.
  void WriteBatch(const WriteOutBatch& records);

  // Gets the lower (inclusive) and upper (exclusive) bounds for the page that
  // `key` should be placed on.
  std::pair<key_utils::KeyHead, key_utils::KeyHead> GetPageBoundsFor(
      key_utils::KeyHead key);

  // Will not be changed after `Initialize()` returns. The objects below are
  // thread safe; no additional mutual exclusion is required.
  Options options_;
  Statistics stats_;
  const std::filesystem::path db_path_;
  std::unique_ptr<RecordCache> rec_cache_;

  // The pointers to the structures below are shared with the record cache.
  std::shared_ptr<BufferManager> buf_mgr_;
  std::shared_ptr<Model> model_;
  std::shared_ptr<ThreadPool> workers_;

  // Remaining database state protected by `mutex_`.
  std::mutex mutex_;

  // Handles reading from and writing to the write-ahead log.
  // REQUIRES: `mutex_` is held when using the manager.
  wal::Manager wal_;
};

}  // namespace tl
