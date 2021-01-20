#include "db_impl.h"

#include <cassert>
#include <condition_variable>
#include <cstdlib>
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
      all_memtables_full_(false) {}

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
  mtable_ = std::make_shared<MemTable>();

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
  return Status::OK();
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
    if (mtable_->HasEntries()) {
      ScheduleMemTableFlush(lock);
    }

    // Not absolutely needed because there should not be any additional writers.
    // But either way, this method should be paired with `WriterWaitIfNeeded()`
    // to keep its usage in the code consistent.
    NotifyWaitingWriterIfNeeded(lock);
  }
  // Deleting the thread pool will block the current thread until the workers
  // have completed all their queued tasks.
  workers_.reset();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return WriteImpl(options, key, value, MemTable::EntryType::kWrite);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::FlushMemTable(const WriteOptions& options) {
  std::shared_future<void> local_last_flush;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    WriterWaitIfNeeded(lock);
    ScheduleMemTableFlush(lock);
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
                         const Slice& value, MemTable::EntryType entry_type) {
  std::unique_lock<std::mutex> lock(mutex_);
  WriterWaitIfNeeded(lock);
  if (ActiveMemTableFull(lock)) {
    ScheduleMemTableFlush(lock);
  }
  Status write_result = mtable_->Add(key, value, entry_type);
  NotifyWaitingWriterIfNeeded(lock);
  return write_result;
}

void DBImpl::ScheduleMemTableFlush(std::unique_lock<std::mutex>& lock) {
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
    // also wait on entry.
    local_last_flush.get();
    lock.lock();
  }

  // Mark the active memtable as immutable and create a new active memtable.
  im_mtable_ = std::move(mtable_);
  mtable_ = std::make_shared<MemTable>();

  // Schedule the flush to run in the background.
  last_flush_ = workers_->Submit([this]() {
    size_t current_page = std::numeric_limits<size_t>::max();
    std::vector<std::future<void>> page_write_futures;
    std::vector<std::pair<const Slice, const Slice>> records_for_page;

    // Iterate through the immutable memtable, aggregate entries into pages,
    // and dispatch page updates.
    auto it = im_mtable_->GetIterator();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      const size_t page_id = model_->KeyToPageId(it.key());
      // The memtable is in sorted order - once we "pass" a page, we won't
      // return to it.
      if (page_id != current_page) {
        if (current_page != std::numeric_limits<size_t>::max()) {
          // Submit flush job to workers - this is not the "first" page.
          page_write_futures.emplace_back(workers_->Submit(
              [this, records_for_page = std::move(records_for_page),
               current_page]() {
                FlushWorker(records_for_page, current_page);
              }));
          records_for_page.clear();
        }
        current_page = page_id;
      }
      records_for_page.emplace_back(std::make_pair(it.key(), it.value()));
    }

    // Flush entries in the last page.
    if (!records_for_page.empty()) {
      assert(current_page != std::numeric_limits<size_t>::max());
      page_write_futures.emplace_back(workers_->Submit(
          [this, records_for_page = std::move(records_for_page),
           current_page]() { FlushWorker(records_for_page, current_page); }));
      records_for_page.clear();
    }

    // Wait for all page updates to complete.
    for (auto& future : page_write_futures) {
      future.get();
    }

    // Flush is complete. We no longer need the immutable memtable.
    {
      std::unique_lock<std::mutex> db_lock(mutex_);
      im_mtable_.reset();
    }
  });

  // At this point the active memtable has space for additional writes. However
  // we do not lower the `all_memtables_full_` flag until all the waiting
  // writers have a chance to complete their writes to ensure they proceed
  // in FIFO order (and to avoid possible starvation of the waiting writers).
}

void DBImpl::FlushWorker(
    const std::vector<std::pair<const Slice, const Slice>>& records,
    size_t page_id) {
  auto& bf = buf_mgr_->FixPage(page_id, /*exclusive = */ true);
  Page page(bf.GetPage());

  for (const auto& kv : records) {
    auto s = page.Put(kv.first, kv.second);
    assert(s.ok());
  }
  buf_mgr_->UnfixPage(bf, /*is_dirty = */ true);
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
