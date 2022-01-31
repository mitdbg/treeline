#include "db_impl.h"

#include <cassert>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <limits>

#include "bufmgr/page_memory_allocator.h"
#include "db/logger.h"
#include "db/manifest.h"
#include "db/page.h"
#include "model/alex_model.h"
#include "model/btree_model.h"
#include "util/affinity.h"
#include "util/key.h"

namespace {

using namespace llsm;

const std::string kWALDirName = "wal";
const std::string kManifestFileName = "MANIFEST";

Status ValidateOptions(const Options& options) {
  if (options.key_hints.key_step_size == 0) {
    return Status::InvalidArgument("KeyDistHints::key_step_size cannot be 0.");
  }
  if (options.key_hints.page_fill_pct < 1 ||
      options.key_hints.page_fill_pct > 100) {
    return Status::InvalidArgument(
        "KeyDistHints::page_fill_pct must be a value between 1 and 100 "
        "inclusive.");
  }
  if (options.buffer_pool_size < options.max_reorg_fanout * Page::kSize) {
    return Status::InvalidArgument(
        "Options::buffer_pool_size is too small. It must be at least " +
        std::to_string(Page::kSize) + " (Page::kSize) * " +
        std::to_string(options.max_reorg_fanout) +
        " (options.max_reorg_fanout) = " +
        std::to_string(options.max_reorg_fanout * Page::kSize) + " bytes.");
  }
  if (options.background_threads < 2) {
    return Status::InvalidArgument(
        "Options::background_threads must be at least 2.");
  }
  if (options.reorg_length < 2) {
    return Status::InvalidArgument("Options::reorg_length must be at least 2");
  }
  return Status::OK();
}

}  // namespace

namespace llsm {

namespace fs = std::filesystem;

Status DB::Open(const Options& options, const fs::path& path, DB** db_out) {
  std::unique_ptr<DBImpl> db = std::make_unique<DBImpl>(options, path);
  Status s = db->Initialize();
  if (!s.ok()) {
    return s;
  }
  *db_out = db.release();
  return Status::OK();
}

DBImpl::DBImpl(const Options options, const fs::path db_path)
    : options_(std::move(options)),
      db_path_(std::move(db_path)),
      buf_mgr_(nullptr),
      model_(nullptr),
      workers_(nullptr),
      rec_cache_(nullptr),
      mtable_(nullptr),
      im_mtable_(nullptr),
      all_memtables_full_(false),
      wal_(db_path_ / kWALDirName),
      mem_budget_memtables_(2 * options_.memtable_flush_threshold),
      mem_budget_(options_.buffer_pool_size + mem_budget_memtables_) {}

Status DBImpl::Initialize() {
  const Status validate = ValidateOptions(options_);
  if (!validate.ok()) return validate;
  try {
    // Check if the DB exists and abort if requested by the user's options.
    const bool db_exists = fs::is_directory(db_path_) &&
                           fs::is_regular_file(db_path_ / kManifestFileName);
    if (db_exists && options_.error_if_exists) {
      return Status::InvalidArgument("DB already exists:", db_path_.string());
    }
    if (!db_exists && !options_.create_if_missing) {
      return Status::InvalidArgument("DB does not exist:", db_path_.string());
    }

    // Set up the thread pool.
    if (options_.pin_threads) {
      std::vector<size_t> core_map;
      core_map.reserve(options_.background_threads);
      for (size_t core_id = 0; core_id < options_.background_threads;
           ++core_id) {
        core_map.push_back(core_id);
      }
      workers_ =
          std::make_unique<ThreadPool>(options_.background_threads, core_map);
    } else {
      workers_ = std::make_unique<ThreadPool>(options_.background_threads);
    }

    // Set up the record cache.
    rec_cache_ = std::make_unique<RecordCache>(options_.record_cache_capacity);

    // Finish initializing the DB based on whether we are creating a completely
    // new DB or if we are opening an existing DB.
    return db_exists ? InitializeExistingDB() : InitializeNewDB();

  } catch (const fs::filesystem_error& ex) {
    return Status::FromPosixError(db_path_.string(), ex.code().value());
  }
}

Status DBImpl::InitializeNewDB() {
  try {
    // No error if the directory already exists.
    fs::create_directory(db_path_);
    if (options_.enable_debug_log) {
      Logger::Initialize(db_path_);
    }
    PageMemoryAllocator::SetAlignmentFor(db_path_);

    const auto values = key_utils::CreateValues<uint64_t>(options_.key_hints);
    const auto records = key_utils::CreateRecords<uint64_t>(values);

    BufMgrOptions bm_options(options_);
    bm_options.num_segments = options_.background_threads;

    if (options_.use_alex) {
      model_ = std::make_unique<ALEXModel>();
    } else {
      model_ = std::make_unique<BTreeModel>();
    }
    buf_mgr_ = std::make_unique<BufferManager>(bm_options, db_path_);

    model_->PreallocateAndInitialize(buf_mgr_, records,
                                     options_.key_hints.records_per_page());
    buf_mgr_->ClearStats();
    stats_.ClearAll();
    Logger::Log("Created new %s. Total size: %zu bytes. Indexed pages: %zu",
                options_.use_alex ? "ALEX" : "BTree", model_->GetSizeBytes(),
                model_->GetNumPages());

    // Write the DB metadata to persistent storage.
    const Status s = Manifest::Builder()
                         .WithNumPages(0)  // Include to not break format
                         .WithNumSegments(bm_options.num_segments)
                         .Build()
                         .WriteTo(db_path_ / kManifestFileName);
    if (!s.ok()) return s;

    return wal_.PrepareForWrite(/*discard_existing_logs=*/true);

  } catch (const fs::filesystem_error& ex) {
    return Status::FromPosixError(db_path_.string(), ex.code().value());
  }
}

Status DBImpl::InitializeExistingDB() {
  if (options_.enable_debug_log) {
    Logger::Initialize(db_path_);
  }
  PageMemoryAllocator::SetAlignmentFor(db_path_);

  Status s;
  const auto manifest = Manifest::LoadFrom(db_path_ / kManifestFileName, &s);
  if (!s.ok()) return s;

  BufMgrOptions bm_options(options_);
  bm_options.num_segments = manifest->num_segments();

  buf_mgr_ = std::make_unique<BufferManager>(bm_options, db_path_);
  if (options_.use_alex) {
    model_ = std::make_unique<ALEXModel>();
  } else {
    model_ = std::make_unique<BTreeModel>();
  }

  model_->ScanFilesAndInitialize(buf_mgr_);
  buf_mgr_->ClearStats();
  stats_.ClearAll();

  Logger::Log("Rebuilt %s. Total size: %zu bytes. Indexed pages: %zu",
              options_.use_alex ? "ALEX" : "BTree", model_->GetSizeBytes(),
              model_->GetNumPages());

  // Before we can accept requests, we need to replay the writes (if any) that
  // exist in the write-ahead log.
  s = wal_.PrepareForReplay();
  if (!s.ok()) return s;

  WriteOptions replay_write_options;
  replay_write_options.bypass_wal = true;  // The writes are already in the WAL.
  s = wal_.ReplayLog(
      [this, &replay_write_options](const Slice& key, const Slice& value,
                                    format::WriteType write_type) {
        if (write_type == format::WriteType::kWrite) {
          return Put(replay_write_options, key, value);
        } else {
          return Delete(replay_write_options, key);
        }
      });
  if (!s.ok()) return s;

  // Make sure any "leftover" WAL writes are persisted.
  // TODO: does this need to be adapted in the face of the record cache?
  if (mtable_->HasEntries()) {
    s = FlushMemTable(/*disable_deferred_io = */ true);
    if (!s.ok()) return s;
  } else {
    // Wait for the in-progress background flush to complete if needed.
    std::unique_lock<std::mutex> lock(mutex_);
    if (FlushInProgress(lock)) {
      assert(last_flush_.valid());
      std::shared_future<void> local_flush = last_flush_;
      lock.unlock();
      local_flush.get();
    }
  }

  return wal_.PrepareForWrite(/*discard_existing_logs=*/true);
}

DBImpl::~DBImpl() {
  std::shared_future<void> pending_flush;

  // Once the destructor is called, no further application threads are allowed
  // to call the `DBImpl`'s public methods.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // Must wait for any earlier writers to get a chance to apply their writes.
    WriterWaitIfNeeded(lock);

    // We must be the last writing thread since no additional public methods can
    // be called.
    assert(waiting_writers_.empty());

    // Any data in the record cache should be flushed to persistent storage. The
    // `RecordCache` destructor will take care of that.
    rec_cache_.reset();

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

  if (buf_mgr_ != nullptr) {  // Might fail initialization and call destructor
                              // before buffer manager has been initialized.
    Logger::Log("Overall buffer manager hit rate: %.4f",
                buf_mgr_->BufMgrHitRate());
  }

  stats_.MoveTempToTotalAndClearTemp();
  Logger::Log(stats_.to_string().c_str());

  Logger::Shutdown();
}

// Reading a value consists of up to two steps:
//
// 1. Search the record cache (`rec_cache_`).
//    - If the record cache contains a `EntryType::kDelete` entry for `key`, we
//    can safely return `Status::NotFound()` because the key was recently
//    deleted.
//    - If the record cache contains a `EntryType::kWrite` entry for `key`, we
//    can return the value directly.
//    - If no entry was found, we move on to the next step.
//
// 2. Search the on-disk page that should store the data associated with `key`,
//    based on the key to page model.
//
// We carry out these steps in this order to ensure we always return the latest
// value associated with a key (i.e., writes always go to the record cache
// first).
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  // 1. Search the record cache.
  uint64_t cache_index;
  Status status =
      rec_cache_->GetCacheIndex(key, /*exclusive = */ false, &cache_index);
  if (status.ok()) {
    ++stats_.temp_user_reads_cache_hits_records_;

    if (RecordCache::cache_entries[cache_index].IsDelete()) {
      return Status::NotFound("Key not found.");
    }
    return Status::OK();
  }

  // 2. Check the on-disk page(s) by following the relevant overflow chain.
  bool next_link_exists = true;
  PhysicalPageId page_id;
  BufferFrame* bf;
  while (true) {
    page_id = model_->KeyToPageId(key);
    bf = &(buf_mgr_->FixPage(page_id, /*exclusive=*/false));
    PhysicalPageId page_id_check = model_->KeyToPageId(key);
    if (page_id_check == page_id) break;  // Double-check for reorg
    buf_mgr_->UnfixPage(*bf, /*is_dirty=*/false);
  }
  bool incurred_io = false;
  bool incurred_multi_io = false;

  while (next_link_exists) {
    next_link_exists = false;
    if (bf->IsNewlyFixed()) {
      incurred_multi_io = incurred_io;
      incurred_io = true;
    }
    Page page = bf->GetPage();
    status = page.Get(key, value_out);
    BufferFrame* old_bf = bf;
    if (status.IsNotFound() && page.HasOverflow()) {
      page_id = page.GetOverflow();
      next_link_exists = true;
      bf = &(buf_mgr_->FixPage(page_id, /*exclusive=*/false));
    }

    buf_mgr_->UnfixPage(*old_bf, /*is_dirty=*/false);
  }

  if (incurred_multi_io) {
    ++stats_.temp_user_reads_multi_bufmgr_misses_records_;
  } else if (incurred_io) {
    ++stats_.temp_user_reads_single_bufmgr_misses_records_;
  } else {
    ++stats_.temp_user_reads_bufmgr_hits_records_;
  }

  return status;
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return WriteImpl(options, key, value, format::WriteType::kWrite);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return WriteImpl(options, key, Slice(), format::WriteType::kDelete);
}

Status DBImpl::BulkLoad(
    const WriteOptions& options,
    std::vector<std::pair<const Slice, const Slice>>& records) {
  if (records.size() == 0) return Status::OK();
  if (!options.sorted_load)
    return Status::InvalidArgument(
        "`options.sorted_load` must be true, indicating that the contents of "
        "`records` are sorted and distinct.");

  // Determine number of needed pages.
  KeyDistHints dist;
  dist.num_keys = records.size();
  dist.key_size = records[0].first.size();  // Assume records are same size.
  dist.record_size = records[0].second.size();

  const size_t needed_pages = dist.num_pages();
  const size_t records_per_page = dist.records_per_page();

  // Fix sole existing page and check that DB is empty.
  const PhysicalPageId sole_page_id = model_->KeyToPageId(records[0].first);
  auto frame = &buf_mgr_->FixPage(sole_page_id, /* exclusive = */ true);

  if (model_->GetNumPages() > 1 || frame->GetPage().GetNumRecords() != 0) {
    buf_mgr_->UnfixPage(*frame, /*is_dirty = */ false);
    return Status::NotSupported("Cannot bulk load a non-empty database");
  }

  // Repurpose the existing page as the first page.
  Page page(frame->GetData(), Slice(std::string(1, 0x00)),
            (records_per_page < records.size())
                ? records.at(records_per_page).first
                : Slice(""));

  // Allocate additional pages and insert records.
  page.Put(options, records[0].first, records[0].second);
  for (size_t i = 1; i < records.size(); ++i) {
    if (i % records_per_page == 0) {  // Switch over to next page.
      buf_mgr_->UnfixPage(*frame, /*is_dirty = */ true);
      const PhysicalPageId page_id = buf_mgr_->GetFileManager()->AllocatePage();
      frame = &buf_mgr_->FixPage(page_id, /* exclusive = */ true,
                                 /* is_newly_allocated = */ true);
      page = Page(frame->GetData(), records.at(i).first,
                  (i + records_per_page < records.size())
                      ? records.at(i + records_per_page).first
                      : Slice(""));
      model_->Insert(records.at(i).first, page_id);
    }
    page.Put(options, records[i].first, records[i].second);
  }

  buf_mgr_->UnfixPage(*frame, /*is_dirty = */ true);
  if (options.flush_dirty_after_bulk) buf_mgr_->FlushDirty();

  return Status::OK();
}

// Gets the number of pages indexed by the model.
size_t DBImpl::GetNumIndexedPages() const { return model_->GetNumPages(); }

Status DBImpl::FlushMemTable(const bool disable_deferred_io) {
  std::shared_future<void> local_last_flush;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    WriterWaitIfNeeded(lock);
    ScheduleMemTableFlush(lock, disable_deferred_io);
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
  return mtable_->ApproximateMemoryUsage() >= mtable_->GetFlushThreshold();
}

bool DBImpl::FlushInProgress(const std::unique_lock<std::mutex>& lock) const {
  assert(lock.owns_lock());
  return im_mtable_ != nullptr;
}

Status DBImpl::WriteImpl(const WriteOptions& options, const Slice& key,
                         const Slice& value, format::WriteType write_type) {
  if (!options.bypass_wal) {
    Status log_result = wal_.LogWrite(options, key, value, write_type);
    if (!log_result.ok()) {
      return log_result;
    }
  }

  Status write_result =
      rec_cache_->Put(key, value, /*is_dirty = */ true, write_type);
  if (write_result.ok()) ++stats_.temp_user_writes_records_;
  return write_result;
}

void DBImpl::ScheduleMemTableFlush(std::unique_lock<std::mutex>& lock,
                                   const bool disable_deferred_io) {
  assert(lock.owns_lock());

  // If a flush is in progress, we need to wait for it to complete before we
  // can schedule another flush. We need to set `all_memtables_full_` to true
  // to ask additional incoming writer threads to wait in line as well.
  if (FlushInProgress(lock)) {
    all_memtables_full_ = true;
    assert(last_flush_.valid());
    std::shared_future<void> local_last_flush(last_flush_);
    lock.unlock();
    // Wait for the in progress flush to complete. We release the database
    // lock to allow reads to proceed concurrently and to allow the flush
    // thread to remove the immutable memtable when it is done. Because
    // `all_memtables_full_` is now true, all additional writer threads will
    // also wait on entry (except writes due to deferred I/O, which will
    // proceed).
    local_last_flush.get();
    lock.lock();
  }

  // Initialize new options
  FlushOptions foptions;
  foptions.disable_deferred_io = disable_deferred_io;
  foptions.deferred_io_max_deferrals = options_.deferred_io_max_deferrals;
  foptions.deferred_io_batch_size = options_.deferred_io_batch_size;

  stats_.MoveTempToTotalAndClearTemp();

  // Increment the log version and get the log version associated with the
  // to-be-flushed memtable.
  const uint64_t flush_log_version = wal_.IncrementLogVersion();

  // Schedule the flush to run in the background.
  last_flush_ = workers_->Submit([this, foptions, flush_log_version]() {
    PhysicalPageId current_page;
    size_t current_page_deferral_count = 0;
    bool current_page_dispatched_fixer = false;
    std::vector<std::future<void>> page_write_futures;
    std::future<OverflowChain> bf_future;
    std::vector<std::tuple<const Slice, const Slice, const format::WriteType>>
        records_for_page;
    size_t bytes_for_page = 0;

    // Iterate through the immutable memtable, aggregate entries into pages,
    // and dispatch page updates.

    // NOTE: We do not need to acquire `mtable_mutex_` here even though we
    // read the `im_mtable_` pointer because only a writing thread modifies
    // `im_mtable_` above. However no writing threads can run that
    // modification code above until this flush completes.
    auto it = im_mtable_->GetIterator();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      const PhysicalPageId page_id = model_->KeyToPageId(it.key());
      // The memtable is in sorted order - once we "pass" a page, we won't
      // return to it.
      if (page_id != current_page) {
        if (current_page.IsValid()) {
          if (ShouldFlush(foptions, bytes_for_page,
                          current_page_deferral_count)) {
            // Submit flush job to workers - this is not the "first" page.
            page_write_futures.emplace_back(workers_->Submit(
                [this, records_for_page = std::move(records_for_page),
                 bf_future = std::move(bf_future), current_page_deferral_count,
                 bytes_for_page]() mutable {
                  FlushWorker(records_for_page, bf_future,
                              current_page_deferral_count, bytes_for_page);
                }));
            records_for_page.clear();
            bytes_for_page = 0;
          } else {
            // Submit re-insertion job to workers.
            page_write_futures.emplace_back(workers_->Submit(
                [this, records_for_page = std::move(records_for_page),
                 current_page_deferral_count]() {
                  ReinsertionWorker(records_for_page,
                                    current_page_deferral_count);
                }));
            records_for_page.clear();
            bytes_for_page = 0;
          }
        }
        current_page = page_id;
        current_page_deferral_count = 0;
        current_page_dispatched_fixer = false;
      }
      records_for_page.emplace_back(
          std::make_tuple(it.key(), it.value(), it.type()));
      bytes_for_page += it.key().size() + it.value().size();

      if (it.seq_num() < foptions.deferred_io_max_deferrals &&
          (it.seq_num() + 1) > current_page_deferral_count)
        current_page_deferral_count = it.seq_num() + 1;

      // As soon as we are sure we will flush to this page, fix it in the
      // background.
      if (ShouldFlush(foptions, bytes_for_page, current_page_deferral_count) &&
          !current_page_dispatched_fixer) {
        bf_future = workers_->Submit([this, current_page]() mutable {
          return FixOverflowChain(current_page,
                                  /*exclusive=*/true,
                                  /*unlock_before_returning=*/true);
        });
        current_page_dispatched_fixer = true;
      }
    }

    // Flush entries in the last page.
    if (ShouldFlush(foptions, bytes_for_page, current_page_deferral_count)) {
      assert(current_page.IsValid());
      page_write_futures.emplace_back(workers_->Submit(
          [this, records_for_page = std::move(records_for_page),
           bf_future = std::move(bf_future), current_page_deferral_count,
           bytes_for_page]() mutable {
            FlushWorker(records_for_page, bf_future,
                        current_page_deferral_count, bytes_for_page);
          }));
      records_for_page.clear();
      bytes_for_page = 0;
    } else {
      page_write_futures.emplace_back(workers_->Submit(
          [this, records_for_page = std::move(records_for_page),
           current_page_deferral_count]() {
            ReinsertionWorker(records_for_page, current_page_deferral_count);
          }));
      records_for_page.clear();
      bytes_for_page = 0;
    }

    // Wait for all page updates to complete.
    for (auto& future : page_write_futures) {
      future.get();
    }

    // Schedule the log version for deletion in the background, if able. Log
    // version numbers start from 0. For example, if we defer a record at most
    // once, a record written in log 0 must have been persisted after the next
    // memtable flush (i.e., when we reach this code and `flush_log_version`
    // is 1). Then we can delete log 0.
    //
    // TODO: Update this in the face of changing deferral parameters
    if (flush_log_version >= foptions.deferred_io_max_deferrals) {
      const uint64_t newest_log_eligible_for_removal =
          flush_log_version - foptions.deferred_io_max_deferrals;
      workers_->SubmitNoWait([this, newest_log_eligible_for_removal]() {
        std::unique_lock<std::mutex> db_lock(mutex_);
        wal_.DiscardOldest(newest_log_eligible_for_removal, &db_lock);
      });
    }
  });

  // At this point the active memtable has space for additional writes.
  // However we do not lower the `all_memtables_full_` flag until all the
  // waiting writers have a chance to complete their writes to ensure they
  // proceed in FIFO order (and to avoid possible starvation of the waiting
  // writers).
}

bool DBImpl::ShouldFlush(const FlushOptions& foptions, size_t batch_size,
                         size_t num_deferrals) const {
  return (foptions.disable_deferred_io ||
          (batch_size >= foptions.deferred_io_batch_size) ||
          (num_deferrals >= foptions.deferred_io_max_deferrals));
}

void DBImpl::FlushWorker(
    const std::vector<
        std::tuple<const Slice, const Slice, const format::WriteType>>& records,
    std::future<OverflowChain>& bf_future, size_t current_page_deferral_count,
    size_t batch_size) {
  auto frames = bf_future.get();

  if (frames == nullptr) {  // A reorg intervened, fall back to reinsertion
    return ReinsertionWorker(records, current_page_deferral_count);
  }

  // Traverse overflow chain, update statistics if requested and lock frames
  // again.
  for (auto& bf : *frames) {
    if (bf->IsNewlyFixed()) {
      ++stats_.temp_flush_bufmgr_misses_pages_;
    } else {
      ++stats_.temp_flush_bufmgr_hits_pages_;
    }

    // Lock the frame again for use. This does not increment the fix count of
    // the frame, i.e. there is no danger of "double-fixing".
    bf->Lock(/*exclusive = */ true);
  }

  // Check whether this batch is so large that, most probably, reorg will be
  // eventually needed.
  const size_t total_fence_bytes =
      frames->at(0)->GetPage().GetLowerBoundary().size() +
      frames->at(0)->GetPage().GetUpperBoundary().size();
  const size_t avg_full_record_size = batch_size / records.size();
  const size_t avg_effective_record_size =
      avg_full_record_size - frames->at(0)->GetPage().GetKeyPrefix().size();
  const size_t
      pages_needed =  // Assumption: existing are on avg as large as new.
      Page::NumPagesNeeded(records.size(), avg_effective_record_size,
                           total_fence_bytes);

  if (pages_needed >= options_.reorg_length) {  // Can play around with this.
    PreorganizeOverflowChain(records, frames, options_.key_hints.page_fill_pct);
    return;
  }

  for (const auto& kv : records) {
    Status s;
    if (std::get<2>(kv) == format::WriteType::kWrite) {  // INSERTION
      // Try to update/insert into existing page in chain
      for (auto& bf : *frames) {
        if (bf->GetPage().HasOverflow()) {
          // Not the last page in the chain; only update or remove.
          s = bf->GetPage().UpdateOrRemove(std::get<0>(kv), std::get<1>(kv));
          if (s.ok()) break;
        } else {
          // Last page in the chain; try inserting.
          s = bf->GetPage().Put(std::get<0>(kv), std::get<1>(kv));
          if (s.ok()) break;

          // Must allocate a new page
          PhysicalPageId new_page_id =
              buf_mgr_->GetFileManager()->AllocatePage();
          auto new_bf = &(buf_mgr_->FixPage(new_page_id, /* exclusive = */ true,
                                            /*is_newly_allocated = */ true));
          Page new_page(new_bf->GetData(), bf->GetPage());
          new_page.MakeOverflow();
          bf->GetPage().SetOverflow(new_page_id);
          frames->push_back(new_bf);

          // Insert into the new page
          s = new_page.Put(std::get<0>(kv), std::get<1>(kv));
          if (!s.ok()) {  // Should never get here.
            std::cerr << "ERROR: Failed to insert into overflow page. Aborting."
                      << std::endl;
            exit(1);
          }
        }
      }

    } else {  // DELETION
      for (auto& bf : *frames) {
        s = bf->GetPage().Delete(std::get<0>(kv));
        if (s.ok()) break;
      }
    }
  }

  // If chain got too long, trigger reorganization
  if (frames->size() >= options_.reorg_length) {
    workers_->SubmitNoWait([this, page_id = frames->at(0)->GetPageId()]() {
      ReorganizeOverflowChain(page_id, options_.key_hints.page_fill_pct);
    });
  }

  // Unfix all
  //
  // TODO: we no longer flush here; wal code needs to be adapted.
  for (auto& bf : *frames) {
    buf_mgr_->UnfixPage(*bf, /* is_dirty = */ true);
  }
}

DBImpl::OverflowChain DBImpl::FixOverflowChain(
    const PhysicalPageId page_id, const bool exclusive,
    const bool unlock_before_returning) {
  BufferFrame* bf;
  PhysicalPageId local_page_id = page_id;

  // Fix first page and check for reorganization
  const size_t pages_before = model_->GetNumPages();
  bf = &(buf_mgr_->FixPage(local_page_id, exclusive));
  const size_t pages_after = model_->GetNumPages();

  if (pages_before != pages_after) {
    buf_mgr_->UnfixPage(*bf, /*is_dirty=*/false);
    return nullptr;
  }

  OverflowChain frames = std::make_unique<std::vector<BufferFrame*>>();
  frames->push_back(bf);

  // Fix all overflow pages in the chain, one by one. This loop will "back
  // off" and retry if it finds out that there are not enough free frames in
  // the buffer pool to fix all the pages in the chain.
  //
  // Note that it is still possible for this loop to cause a livelock if all
  // threads end up fixing and unfixing their pages in unison.
  while (frames->back()->GetPage().HasOverflow()) {
    local_page_id = frames->back()->GetPage().GetOverflow();
    bf = buf_mgr_->FixPageIfFrameAvailable(local_page_id, exclusive);
    if (bf == nullptr) {
      // No frames left. We should unfix all overflow pages in this chain
      // (i.e., all pages except the first page in the chain) to give other
      // workers a chance to fix their entire chain.
      while (frames->size() > 1) {
        buf_mgr_->UnfixPage(*(frames->back()), /*is_dirty=*/false);
        frames->pop_back();
      }
      assert(frames->size() == 1);
      // Retry from the beginning of the chain.
      continue;
    }
    frames->push_back(bf);
  }

  if (unlock_before_returning) {
    for (auto& bf : *frames) {
      // Unlock the frame so that it can be "handed over" to the caller. This
      // does not decrement the fix count of the frame, i.e. there's no danger
      // of eviction before we can use it.
      bf->Unlock();
    }
  }

  return frames;
}

void DBImpl::ReinsertionWorker(
    const std::vector<
        std::tuple<const Slice, const Slice, const format::WriteType>>& records,
    size_t current_page_deferral_count) {
  // NOTE: We do not need to acquire `mtable_mutex_` here even though we read
  // the `mtable_` pointer because this code runs as part of the memtable
  // flush. Until this worker completes, no other threads are able to modify
  // `mtable_`. Acquiring `mutex_` ensures writes to the memtable do not occur
  // concurrently.
  ++stats_.temp_flush_deferred_pages_;
  stats_.temp_flush_deferred_records_ += records.size();

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
