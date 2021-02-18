#include "db_impl.h"

#include <cassert>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <limits>

#include "db/page.h"
#include "util/affinity.h"
#include "util/key.h"

namespace llsm {

Status DB::Open(const Options& options, const std::string& path, DB** db_out) {
  std::unique_ptr<DBImpl> db = std::make_unique<DBImpl>(options, path);
  Status s = db->Initialize();
  if (!s.ok()) {
    return s;
  }
  *db_out = db.release();
  return Status::OK();
}

DBImpl::DBImpl(Options options, std::string db_path)
    : options_(std::move(options)),
      db_path_(std::move(db_path)),
      buf_mgr_(nullptr),
      model_(nullptr),
      workers_(nullptr),
      mtable_(nullptr),
      im_mtable_(nullptr),
      all_memtables_full_(false),
      wal_(db_path_ + "/wal") {}

Status DBImpl::Initialize() {
  if (options_.key_step_size == 0) {
    return Status::InvalidArgument("Options::key_step_size cannot be 0.");
  }
  if (options_.page_fill_pct < 1 || options_.page_fill_pct > 100) {
    return Status::InvalidArgument(
        "Options::page_fill_pct must be a value between 1 and 100 inclusive.");
  }
  if (options_.buffer_pool_size < Page::kSize) {
    return Status::InvalidArgument(
        "Options::buffer_pool_size is too small. It must be at least " +
        std::to_string(Page::kSize) + " bytes.");
  }
  if (options_.background_threads < 2) {
    return Status::InvalidArgument(
        "Options::background_threads must be at least 2.");
  }

  // Create directory and an appropriate number of files (segments), one per
  // worker thread.
  if (mkdir(db_path_.c_str(), 0755) != 0) {
    return Status::IOError("Failed to create new directory:", db_path_);
  }

  auto values = key_utils::CreateValues<uint64_t>(options_);
  auto records = key_utils::CreateRecords<uint64_t>(values);

  // Compute the number of records per page.
  double fill_pct = options_.page_fill_pct / 100.;
  options_.records_per_page = Page::kSize * fill_pct / options_.record_size;

  RSModel* model = new RSModel(options_, records);
  buf_mgr_ = std::make_unique<BufferManager>(options_, db_path_);

  model->Preallocate(records, buf_mgr_);
  model_.reset(model);
  mtable_ = std::make_shared<MemTable>(options_.deferred_io_max_deferrals);

  if (options_.pin_threads) {
    std::vector<size_t> core_map;
    core_map.reserve(options_.background_threads);
    for (size_t core_id = 0; core_id < options_.background_threads; ++core_id) {
      core_map.push_back(core_id);
    }
    workers_ =
        std::make_unique<ThreadPool>(options_.background_threads, core_map);
  } else {
    workers_ = std::make_unique<ThreadPool>(options_.background_threads);
  }

  // TODO: Replay the log when opening an existing LLSM database. (Task #2)
  return wal_.PrepareForWrite(/*discard_existing_logs=*/true);
}

DBImpl::~DBImpl() {
  // Once the destructor is called, no further application threads are allowed
  // to call the `DBImpl`'s public methods.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // Must wait for any earlier writers to get a chance to apply their writes.
    WriterWaitIfNeeded(lock);

    // We must be the last writing thread since no additional public methods can
    // be called.
    assert(waiting_writers_.empty());

    // Any data in the active memtable should be flushed to persistent storage.
    FlushOptions options;
    options.disable_deferred_io = true;
    if (mtable_ && mtable_->HasEntries()) {
      ScheduleMemTableFlush(options, lock);
    }

    // Not absolutely needed because there should not be any additional writers.
    // But either way, this method should be paired with `WriterWaitIfNeeded()`
    // to keep its usage in the code consistent.
    NotifyWaitingWriterIfNeeded(lock);
  }
  // Deleting the thread pool will block the current thread until the workers
  // have completed all their queued tasks.
  workers_.reset();

  // All volatile data has been flushed to persistent storage. We can now
  // safely discard the write-ahead log.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    wal_.DiscardAllForCleanShutdown();
  }
}

// Reading a value consists of up to four steps:
//
// 1. Make copies of the memtable pointers so that we can search them without
//     holding the `mutex_`.
//
// 2. Search the active memtable (`mtable_`).
//    - We do not need to hold the `mutex_` because the memtable's underlying
//      data structure (a skip list) supports concurrent reads during a write.
//    - If the memtable contains a `EntryType::kDelete` entry for `key`, we can
//      safely return `Status::NotFound()` because the key was recently deleted.
//    - If the memtable contains a `EntryType::kWrite` entry for `key`, we can
//      return the value directly.
//    - If no entry was found, we move on to the next step.
//
// 3. Search the currently-being-flushed memtable (`im_mtable_`), if it exists.
//    - We do not need to hold the `mutex_` during the search because the
//      currently-being-flushed memtable is immutable.
//    - We use the same search protocol as specified in step 2.
//
// 4. Search the on-disk page that should store the data associated with `key`,
//    based on the key to page model.
//    - We do not need to hold the `mutex_` during the search because the buffer
//      manager manages the mutual exclusion for us.
//
// We carry out these steps in this order to ensure we always return the latest
// value associated with a key (i.e., writes always go to the memtable first).
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  std::shared_ptr<MemTable> local_mtable = nullptr;
  std::shared_ptr<MemTable> local_im_mtable = nullptr;

  // 1. Get copies of the memtable pointers so we can search them without
  //    holding the database lock.
  {
    std::unique_lock<std::mutex> mtable_lock(mtable_mutex_);
    local_mtable = mtable_;
    local_im_mtable = im_mtable_;
  }

  // 2. Search the active memtable.
  format::WriteType write_type;
  Status status = local_mtable->Get(key, &write_type, value_out);
  if (status.ok()) {
    if (write_type == format::WriteType::kDelete) {
      return Status::NotFound("Key not found.");
    }
    return Status::OK();
  }

  // 3. Check the immutable memtable, if it exists.
  if (local_im_mtable != nullptr) {
    status = local_im_mtable->Get(key, &write_type, value_out);
    if (status.ok()) {
      if (write_type == format::WriteType::kDelete) {
        return Status::NotFound("Key not found.");
      }
      return Status::OK();
    }
  }

  // 4. Check the on-disk page.
  size_t page_id = model_->KeyToPageId(key);
  auto& bf = buf_mgr_->FixPage(page_id, /*exclusive=*/false);
  status = bf.GetPage().Get(key, value_out);
  buf_mgr_->UnfixPage(bf, /*is_dirty=*/false);

  return status;
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return WriteImpl(options, key, value, format::WriteType::kWrite);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return WriteImpl(options, key, Slice(), format::WriteType::kDelete);
}

Status DBImpl::FlushMemTable(const FlushOptions& options) {
  std::shared_future<void> local_last_flush;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    WriterWaitIfNeeded(lock);
    ScheduleMemTableFlush(options, lock);
    local_last_flush = last_flush_;
    NotifyWaitingWriterIfNeeded(lock);
  }
  assert(local_last_flush.valid());
  // Wait for the flush to complete before returning.
  local_last_flush.get();
  return Status::OK();
}

bool DBImpl::ActiveMemTableFull(
    const std::unique_lock<std::mutex>& lock) const {
  assert(lock.owns_lock());
  return mtable_->ApproximateMemoryUsage() >= options_.memtable_flush_threshold;
}

bool DBImpl::FlushInProgress(const std::unique_lock<std::mutex>& lock) const {
  assert(lock.owns_lock());
  return im_mtable_ != nullptr;
}

Status DBImpl::WriteImpl(const WriteOptions& options, const Slice& key,
                         const Slice& value, format::WriteType write_type) {
  std::unique_lock<std::mutex> lock(mutex_);
  WriterWaitIfNeeded(lock);
  if (ActiveMemTableFull(lock)) {
    ScheduleMemTableFlush(FlushOptions(), lock);
  }
  if (!options.bypass_wal) {
    Status log_result = wal_.LogWrite(options, key, value, write_type);
    if (!log_result.ok()) {
      NotifyWaitingWriterIfNeeded(lock);
      return log_result;
    }
  }
  // NOTE: We do not need to acquire `mtable_mutex_` here even though we read
  // the `mtable_` pointer because only a writing thread can modify `mtable_`.
  // Since we are currently holding `mutex_`, no other writing thread can
  // concurrently modify `mtable_`.
  Status write_result = mtable_->Add(key, value, write_type);
  NotifyWaitingWriterIfNeeded(lock);
  return write_result;
}

void DBImpl::ScheduleMemTableFlush(const FlushOptions& options,
                                   std::unique_lock<std::mutex>& lock) {
  assert(lock.owns_lock());

  // If a flush is in progress, we need to wait for it to complete before we can
  // schedule another flush. We need to set `all_memtables_full_` to true to ask
  // additional incoming writer threads to wait in line as well.
  if (FlushInProgress(lock)) {
    all_memtables_full_ = true;
    assert(last_flush_.valid());
    std::shared_future<void> local_last_flush(last_flush_);
    lock.unlock();
    // Wait for the in progress flush to complete. We release the database lock
    // to allow reads to proceed concurrently and to allow the flush thread to
    // remove the immutable memtable when it is done. Because
    // `all_memtables_full_` is now true, all additional writer threads will
    // also wait on entry (except writes due to deferred I/O, which will
    // proceed).
    local_last_flush.get();
    lock.lock();
  }

  // Mark the active memtable as immutable and create a new active memtable.
  {
    std::unique_lock<std::mutex> mtable_lock(mtable_mutex_);
    im_mtable_ = std::move(mtable_);
    mtable_ = std::make_shared<MemTable>(options_.deferred_io_max_deferrals);
  }

  // Increment the log version and get the log version associated with the
  // to-be-flushed memtable.
  const uint64_t flush_log_version = wal_.IncrementLogVersion();

  // Schedule the flush to run in the background.
  last_flush_ = workers_->Submit([this, options, flush_log_version]() {
    size_t current_page = std::numeric_limits<size_t>::max();
    size_t current_page_deferral_count = 0;
    bool current_page_dispatched_fixer = false;
    std::vector<std::future<void>> page_write_futures;
    std::future<BufferFrame*> bf_future;
    std::vector<std::tuple<const Slice, const Slice, const format::WriteType>>
        records_for_page;

    // Iterate through the immutable memtable, aggregate entries into pages,
    // and dispatch page updates.

    // NOTE: We do not need to acquire `mtable_mutex_` here even though we read
    // the `im_mtable_` pointer because only a writing thread modifies
    // `im_mtable_` above. However no writing threads can run that modification
    // code above until this flush completes.
    auto it = im_mtable_->GetIterator();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      const size_t page_id = model_->KeyToPageId(it.key());
      // The memtable is in sorted order - once we "pass" a page, we won't
      // return to it.
      if (page_id != current_page) {
        if (current_page != std::numeric_limits<size_t>::max()) {
          if (ShouldFlush(options, records_for_page.size(),
                          current_page_deferral_count)) {
            // Submit flush job to workers - this is not the "first" page.
            page_write_futures.emplace_back(workers_->Submit(
                [this, options, records_for_page = std::move(records_for_page),
                 bf_future = std::move(bf_future)]() mutable {
                  FlushWorker(records_for_page, bf_future);
                }));
            records_for_page.clear();
          } else {
            // Submit re-insertion job to workers.
            page_write_futures.emplace_back(workers_->Submit(
                [this, records_for_page = std::move(records_for_page),
                 current_page_deferral_count]() {
                  ReinsertionWorker(records_for_page,
                                    current_page_deferral_count);
                }));
            records_for_page.clear();
          }
        }
        current_page = page_id;
        current_page_deferral_count = 0;
        current_page_dispatched_fixer = false;
      }
      records_for_page.emplace_back(
          std::make_tuple(it.key(), it.value(), it.type()));

      if (it.seq_num() < options_.deferred_io_max_deferrals &&
          (it.seq_num() + 1) > current_page_deferral_count)
        current_page_deferral_count = it.seq_num() + 1;

      // As soon as we are sure we will flush to this page, fix it in the
      // background.
      if (ShouldFlush(options, records_for_page.size(),
                      current_page_deferral_count) &&
          !current_page_dispatched_fixer) {
        std::promise<BufferFrame*> bf_promise;
        bf_future = bf_promise.get_future();
        workers_->SubmitNoWait(
            [this, current_page, bf_promise = std::move(bf_promise)]() mutable {
              FixWorker(current_page, bf_promise);
            });
        current_page_dispatched_fixer = true;
      }
    }

    // Flush entries in the last page.
    if (ShouldFlush(options, records_for_page.size(),
                    current_page_deferral_count)) {
      assert(current_page != std::numeric_limits<size_t>::max());
      page_write_futures.emplace_back(workers_->Submit(
          [this, options, records_for_page = std::move(records_for_page),
           bf_future = std::move(bf_future)]() mutable {
            FlushWorker(records_for_page, bf_future);
          }));
      records_for_page.clear();
    } else {
      page_write_futures.emplace_back(workers_->Submit(
          [this, records_for_page = std::move(records_for_page),
           current_page_deferral_count]() {
            ReinsertionWorker(records_for_page, current_page_deferral_count);
          }));
      records_for_page.clear();
    }

    // Wait for all page updates to complete.
    for (auto& future : page_write_futures) {
      future.get();
    }

    // Flush is complete. We no longer need the immutable memtable.
    {
      std::unique_lock<std::mutex> mtable_lock(mtable_mutex_);
      im_mtable_.reset();
    }

    // Schedule the log version for deletion in the background, if able. Log
    // version numbers start from 0. For example, if we defer a record at most
    // once, a record written in log 0 must have been persisted after the next
    // memtable flush (i.e., when we reach this code and `flush_log_version` is
    // 1). Then we can delete log 0.
    if (flush_log_version >= options_.deferred_io_max_deferrals) {
      const uint64_t newest_log_eligible_for_removal =
          flush_log_version - options_.deferred_io_max_deferrals;
      workers_->SubmitNoWait([this, newest_log_eligible_for_removal]() {
        std::unique_lock<std::mutex> db_lock(mutex_);
        wal_.DiscardOldest(newest_log_eligible_for_removal, &db_lock);
      });
    }
  });

  // At this point the active memtable has space for additional writes. However
  // we do not lower the `all_memtables_full_` flag until all the waiting
  // writers have a chance to complete their writes to ensure they proceed
  // in FIFO order (and to avoid possible starvation of the waiting writers).
}

bool DBImpl::ShouldFlush(const FlushOptions& options, size_t num_records,
                         size_t num_deferrals) const {
  return (options.disable_deferred_io ||
          (num_records >= options_.deferred_io_min_entries) ||
          (num_deferrals >= options_.deferred_io_max_deferrals));
}

void DBImpl::FlushWorker(
    const std::vector<
        std::tuple<const Slice, const Slice, const format::WriteType>>& records,
    std::future<BufferFrame*>& bf_future) {
  auto bf = bf_future.get();
  // Lock the frame again for use. This does not increment the fix count of
  // the frame, i.e. there is no danger of "double-fixing".
  bf->Lock(/*exclusive = */ true);
  Page page(bf->GetPage());

  for (const auto& kv : records) {
    Status s;
    if (std::get<2>(kv) == format::WriteType::kWrite) {
      s = page.Put(WriteOptions(), std::get<0>(kv), std::get<1>(kv));
    } else {
      s = page.Delete(std::get<0>(kv));
    }
    if (s.IsInvalidArgument()) {
      // TODO: Handle full pages. For now we should force an exit here to
      // prevent a silent write failure in our benchmarks (assertions are
      // disabled in release builds).
      std::cerr << "ERROR: Attempted to write to a full page. Aborting."
                << std::endl;
      exit(1);
    }
  }
  buf_mgr_->FlushAndUnfixPage(*bf);
}

void DBImpl::FixWorker(size_t page_id, std::promise<BufferFrame*>& bf_promise) {
  BufferFrame& bf = buf_mgr_->FixPage(page_id, /*exclusive = */ true);
  // Unlock the frame so that it can be "handed over" to a FlushWorker thread.
  // This does not decrement the fix count of the frame, i.e. there's no danger
  // of eviction before we can use it.
  bf.Unlock();
  bf_promise.set_value(&bf);
}

void DBImpl::ReinsertionWorker(
    const std::vector<
        std::tuple<const Slice, const Slice, const format::WriteType>>& records,
    size_t current_page_deferral_count) {
  // NOTE: We do not need to acquire `mtable_mutex_` here even though we read
  // the `mtable_` pointer because this code runs as part of the memtable flush.
  // Until this worker completes, no other threads are able to modify `mtable_`.
  // Acquiring `mutex_` ensures writes to the memtable do not occur
  // concurrently.
  std::unique_lock<std::mutex> lock(mutex_);
  for (auto& kv : records) {
    // This add will proceed even if mtable_ appears full to regular writers.
    Status s =
        mtable_->Add(std::get<0>(kv), std::get<1>(kv), std::get<2>(kv),
                     /* from_deferral = */ true,
                     /* injected_sequence_num = */ current_page_deferral_count);
    assert(s.ok());
  }
}

class DBImpl::WaitingWriter {
 public:
  // Called by the writer thread to wait until it can proceed.
  // REQUIRES: `mutex_` is held.
  void Wait(std::unique_lock<std::mutex>& lock) {
    cv_.wait(lock, [this]() { return can_proceed_; });
  }

  // Called by a different thread to notify the waiting writer that it can
  // proceed.
  // REQUIRES: `mutex_` is held.
  void Notify(const std::unique_lock<std::mutex>& lock) {
    assert(lock.owns_lock());
    can_proceed_ = true;
    cv_.notify_one();
  }

 private:
  // The waiting writer thread waits on this condition variable until it is
  // notified to proceed by a different thread.
  std::condition_variable cv_;
  // Needed to handle spurious wakeup.
  bool can_proceed_ = false;
};

void DBImpl::WriterWaitIfNeeded(std::unique_lock<std::mutex>& lock) {
  assert(lock.owns_lock());
  if (!all_memtables_full_) {
    return;
  }

  // All the memtables are full, so we need to wait in line before we can make
  // any modifications.
  WaitingWriter this_writer;
  waiting_writers_.push(&this_writer);
  this_writer.Wait(lock);
  assert(waiting_writers_.front() == &this_writer);
  waiting_writers_.pop();
}

void DBImpl::NotifyWaitingWriterIfNeeded(
    const std::unique_lock<std::mutex>& lock) {
  assert(lock.owns_lock());
  if (!waiting_writers_.empty()) {
    // The next writer can proceed.
    waiting_writers_.front()->Notify(lock);

  } else {
    // There are no additional waiting writers. We need to ensure that the
    // `all_memtables_full_` flag is lowered now.
    all_memtables_full_ = false;
  }
}

}  // namespace llsm
