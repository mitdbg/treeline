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
      wal_(db_path_ / kWALDirName) {}

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
          std::make_shared<ThreadPool>(options_.background_threads, core_map);
    } else {
      workers_ = std::make_shared<ThreadPool>(options_.background_threads);
    }

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

    buf_mgr_ = std::make_shared<BufferManager>(bm_options, db_path_);
    if (options_.use_alex) {
      model_ = std::make_shared<ALEXModel>();
    } else {
      model_ = std::make_shared<BTreeModel>();
    }
    rec_cache_ = std::make_unique<RecordCache>(&options_, &stats_, model_,
                                               buf_mgr_, workers_);

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

  buf_mgr_ = std::make_shared<BufferManager>(bm_options, db_path_);
  if (options_.use_alex) {
    model_ = std::make_shared<ALEXModel>();
  } else {
    model_ = std::make_shared<BTreeModel>();
  }
  rec_cache_ = std::make_unique<RecordCache>(&options_, &stats_, model_,
                                             buf_mgr_, workers_);

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

  // Make sure any "leftover" WAL writes are persisted
  s = FlushRecordCache(/*disable_deferred_io = */ true);
  if (!s.ok()) return s;
  buf_mgr_->FlushDirty();

  return wal_.PrepareForWrite(/*discard_existing_logs=*/true);
}

DBImpl::~DBImpl() {
  // Any data in the record cache should be flushed to persistent storage. The
  // `RecordCache` destructor will take care of that.
  rec_cache_.reset();

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
  auto entry = &RecordCache::cache_entries[cache_index];
  if (status.ok()) {
    ++stats_.temp_user_reads_cache_hits_records_;

    if (entry->IsDelete()) {
      return Status::NotFound("Key not found.");
    }

    value_out->assign(entry->GetValue().data(), entry->GetValue().size());
    entry->Unlock();
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
  dist.record_size = dist.key_size + records[0].second.size();

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

Status DBImpl::FlushRecordCache(const bool disable_deferred_io) {
  rec_cache_->WriteOutDirty(options_.reorg_length,
                            options_.key_hints.page_fill_pct);
  return Status::OK();
}

Status DBImpl::WriteImpl(const WriteOptions& options, const Slice& key,
                         const Slice& value, format::WriteType write_type) {
  if (!options.bypass_wal) {
    Status log_result = wal_.LogWrite(options, key, value, write_type);
    if (!log_result.ok()) {
      return log_result;
    }
  }

  PhysicalPageId chain_to_reorg;
  uint64_t chain_size = 0;
  Status write_result =
      rec_cache_->Put(key, value, /*is_dirty = */ true, write_type, 4, true,
                      options_.reorg_length, options_.key_hints.page_fill_pct);
  if (write_result.ok()) ++stats_.temp_user_writes_records_;

  return write_result;
}

}  // namespace llsm
