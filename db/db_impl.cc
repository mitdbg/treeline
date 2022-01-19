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
  if (options.buffer_pool_size < Page::kSize) {
    return Status::InvalidArgument(
        "Options::buffer_pool_size is too small. It must be at least " +
        std::to_string(Page::kSize) + " bytes.");
  }
  if (options.background_threads < 2) {
    return Status::InvalidArgument(
        "Options::background_threads must be at least 2.");
  }
  if (options.reorg_length < 2) {
    return Status::InvalidArgument("Options::reorg_length must be at least 2");
  }
  size_t min_memory_budget = Page::kSize * (options.max_reorg_fanout + 2);
  if (options.buffer_pool_size < min_memory_budget) {
    return Status::InvalidArgument(
        "Buffer pool memory budget must be at least Page::kSize * "
        "(options.max_reorg_fanout + 2)");
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
      wal_(db_path_ / kWALDirName),
      mem_budget_(options_.buffer_pool_size) {}

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

  return wal_.PrepareForWrite(/*discard_existing_logs=*/true);
}

DBImpl::~DBImpl() {
  std::shared_future<void> pending_flush;

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

// Reading a value consists of searching the on-disk page that should store the
// data associated with `key`, based on the key to page model.
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  Status status;
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

Status DBImpl::WriteImpl(const WriteOptions& options, const Slice& key,
                         const Slice& value, format::WriteType write_type) {
  if (!options.bypass_wal) {
    Status log_result = wal_.LogWrite(options, key, value, write_type);
    if (!log_result.ok()) {
      return log_result;
    }
  }

  BufferFrame* bf;
  PhysicalPageId page_id;

  OverflowChain frames = nullptr;
  while (frames == nullptr) {
    page_id = model_->KeyToPageId(key);
    frames = FixOverflowChain(page_id,
                              /*exclusive=*/true,
                              /*unlock_before_returning=*/false);
  }

  // Fix all overflow pages in the chain, one by one. This loop will "back
  // off" and retry if it finds out that there are not enough free frames in
  // the buffer pool to fix all the pages in the chain.
  //
  // Note that it is still possible for this loop to cause a livelock if all
  // threads end up fixing and unfixing their pages in unison.
  while (frames->back()->GetPage().HasOverflow()) {
    page_id = frames->back()->GetPage().GetOverflow();
    bf = buf_mgr_->FixPageIfFrameAvailable(page_id, /*exclusive = */ true);
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

  Status s;
  if (write_type == format::WriteType::kWrite) {  // INSERTION
    // Try to update/insert into existing page in chain
    for (auto& bf : *frames) {
      if (bf->GetPage().HasOverflow()) {
        // Not the last page in the chain; only update or remove.
        s = bf->GetPage().UpdateOrRemove(key, value);
        if (s.ok()) break;
      } else {
        // Last page in the chain; try inserting.
        s = bf->GetPage().Put(key, value);
        if (s.ok()) break;

        // Must allocate a new page
        PhysicalPageId new_page_id = buf_mgr_->GetFileManager()->AllocatePage();
        auto new_bf = &(buf_mgr_->FixPage(new_page_id, /* exclusive = */ true,
                                          /*is_newly_allocated = */ true));
        Page new_page(new_bf->GetData(), bf->GetPage());
        new_page.MakeOverflow();
        bf->GetPage().SetOverflow(new_page_id);
        frames->push_back(new_bf);

        // Insert into the new page
        s = new_page.Put(key, value);
        if (!s.ok()) {  // Should never get here.
          std::cerr << "ERROR: Failed to insert into overflow page. Aborting."
                    << std::endl;
          exit(1);
        }
      }
    }

  } else {  // DELETION
    for (auto& bf : *frames) {
      s = bf->GetPage().Delete(key);
      if (s.ok()) break;
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

  if (s.ok()) ++stats_.temp_user_writes_records_;
  stats_.MoveTempToTotalAndClearTemp();

  // TODO: Deal with write-ahead logging.

  return s;
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

}  // namespace llsm
