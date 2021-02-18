#pragma once

#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <queue>

#include "bufmgr/buffer_manager.h"
#include "db/format.h"
#include "db/memtable.h"
#include "llsm/db.h"
#include "model/rs_model.h"
#include "util/thread_pool.h"
#include "wal/manager.h"

namespace llsm {

class DBImpl : public DB {
 public:
  DBImpl(Options options, std::string db_path);
  ~DBImpl() override;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value_out) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status FlushMemTable(const FlushOptions& options) override;

  // Must be called exactly once after `DBImpl` is constructed to initialize the
  // database's internal state. Other public `DBImpl` methods can be called
  // after and only if this method returns `Status::OK()`.
  Status Initialize();

 private:
  // Records writes and deletes in the active `MemTable`. The `value` is ignored
  // if `write_type` is `WriteType::kDelete`.
  // This method is thread safe.
  Status WriteImpl(const WriteOptions& options, const Slice& key,
                   const Slice& value, format::WriteType write_type);

  // Schedules a flush of the active memtable in the background. The active
  // memtable will be made immutable and a new active memtable will be
  // constructed.
  //
  // Only one flush can be pending at any time. If a flush is currently already
  // in progress, this method will block and wait until that flush completes
  // before scheduling the next flush.
  //
  // REQUIRES: `mutex_` is held.
  // REQUIRES: The thread has already called `WriterWaitIfNeeded()`.
  void ScheduleMemTableFlush(const FlushOptions& options,
                             std::unique_lock<std::mutex>& lock);

  // Returns true iff `mtable_` is "full".
  // REQUIRES: `mutex_` is held.
  bool ActiveMemTableFull(const std::unique_lock<std::mutex>& lock) const;

  // Returns true iff `im_mtable_` is being flushed.
  // REQUIRES: `mutex_` is held.
  bool FlushInProgress(const std::unique_lock<std::mutex>& lock) const;

  bool ShouldFlush(const FlushOptions& options, size_t num_records,
                   size_t num_deferrals) const;

  // Code run by a worker thread to write out `records` to the page held by
  // `bf`.
  void FlushWorker(
      const std::vector<std::tuple<const Slice, const Slice,
                                   const format::WriteType>>& records,
      std::future<BufferFrame*>& bf_future);

  // Code run by a worker thread to fix the page with `page_id`.
  void FixWorker(size_t page_id, std::promise<BufferFrame*>& bf_promise);

  // Code run by a worker thread to reinsert `records` into the now-active
  // memtable if their flush was deferred.
  void ReinsertionWorker(
      const std::vector<std::tuple<const Slice, const Slice,
                                   const format::WriteType>>& records,
      size_t current_page_deferral_count);

  // All writing threads must call this method "on entry" to ensure they wait if
  // needed (when the memtables are all full).
  // REQUIRES: `mutex_` is held.
  void WriterWaitIfNeeded(std::unique_lock<std::mutex>& lock);

  // A writing thread that has finished its work should call this method "before
  // exiting" to wake up the next waiting writer thread, if any.
  // REQUIRES: `mutex_` is held.
  void NotifyWaitingWriterIfNeeded(const std::unique_lock<std::mutex>& lock);

  // Will not be changed after `Initialize()` returns. The objects below are
  // thread safe; no additional mutual exclusion is required.
  Options options_;
  const std::string db_path_;
  std::unique_ptr<BufferManager> buf_mgr_;
  std::unique_ptr<Model> model_;
  std::unique_ptr<ThreadPool> workers_;

  // Remaining database state protected by `mutex_`.
  std::mutex mutex_;

  // Protects the `mtable_` and `im_mtable_` pointers only.
  // If this mutex needs to be acquired with `mutex_` above, always acquire
  // `mutex_` first to prevent circular waits.
  std::mutex mtable_mutex_;

  // Active memtable that may accept writes. After `Initialize()` returns,
  // this pointer will never be null.
  // REQUIRES:
  //  Writing Thread:
  //   - `mutex_` is held when writing to the memtable itself
  //   - `mtable_mutex_` is held when writing to the pointer
  //  Reading Thread:
  //   - `mtable_mutex_` is held when reading the pointer (making a copy)
  std::shared_ptr<MemTable> mtable_;

  // Immutable memtable currently being flushed, if not null. Writes to this
  // table are not allowed. Reads of this table can occur concurrently iff the
  // reading thread has its own copy of the shared pointer.
  // REQUIRES: `mtable_mutex_` is held for read/write/copy of the *pointer* only.
  std::shared_ptr<MemTable> im_mtable_;

  // Is set to true when both `mtable_` and `im_mtable_` are full (i.e.,
  // `mtable_` is full and `im_mtable_` is still being flushed).
  // REQUIRES: `mutex_` is held (for read/write).
  bool all_memtables_full_;

  // A `future` used to wait for the most recent flush to complete. Waiting on
  // the future can be safely done without holding any locks as long as a copy
  // is made. This future is guaranteed to be valid after
  // `ScheduleMemTableFlush()` has executed once.
  // REQUIRES: `mutex_` is held when reading/modifying this future.
  std::shared_future<void> last_flush_;

  class WaitingWriter;
  // Queue used to "hold" writing threads that need to wait because all the
  // memtables are full.
  // REQUIRES: `mutex_` is held (for read/write).
  std::queue<WaitingWriter*> waiting_writers_;

  // Handles reading from and writing to the write-ahead log.
  // REQUIRES: `mutex_` is held when using the manager.
  wal::Manager wal_;
};

}  // namespace llsm
