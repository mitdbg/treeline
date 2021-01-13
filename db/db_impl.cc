#include "db_impl.h"

#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "db/page.h"
#include "util/affinity.h"
#include "util/key.h"

namespace {

class ThreadPool {
 public:
  explicit ThreadPool(size_t num_threads) : shutdown_(false) {
    for (size_t i = 0; i < num_threads; ++i) {
      workers_.emplace_back(&ThreadPool::WorkerMain, this, i + 1);
    }
  }

  ~ThreadPool() {
    {
      std::unique_lock lock(mutex_);
      shutdown_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
      worker.join();
    }
  }

  bool Submit(std::function<void(void)> job) {
    std::unique_lock lock(mutex_);
    if (shutdown_) {
      return false;
    }
    work_queue_.push(job);
    return true;
  }

 private:
  void WorkerMain(uint32_t id) {
    llsm::affinity::PinToCore(id);
    std::function<void(void)> next_job;
    while (true) {
      {
        std::unique_lock lock(mutex_);
        // Need a loop here to handle spurious wakeup
        while (!shutdown_ && work_queue_.empty()) {
          cv_.wait(lock);
        }
        if (shutdown_ && work_queue_.empty()) break;
        next_job = work_queue_.front();
        work_queue_.pop();
      }
      next_job();
    }
  }

  std::vector<std::thread> workers_;

  std::mutex mutex_;
  std::condition_variable cv_;
  bool shutdown_;
  std::queue<std::function<void(void)>> work_queue_;
};

}  // namespace

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
      mtable_(std::make_unique<MemTable>()) {}

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

  return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return mtable_->Put(key, value);
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::FlushMemTable(const WriteOptions& options) {
  affinity::PinInScope run_on_core(0);
  {
    ThreadPool workers(options_.num_flush_threads);
    uint32_t current_page = UINT32_MAX;
    std::vector<std::pair<const Slice, const Slice>>
        records_for_page;  // FIXME: This should have Slice*, not Slice, to
                           // limit memory use.

    auto it = mtable_->GetIterator();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      size_t page_id = model_->KeyToPageId(it.key());
      // The memtable is in sorted order - once we "pass" a page, we won't
      // return to it
      if (page_id != current_page) {
        if (current_page != UINT32_MAX) {
          // Submit flush job to workers - this is not the "first" page
          workers.Submit([records_for_page(std::move(records_for_page)), this,
                          current_page]() {
            FlushWorkerMain(records_for_page, current_page);
          });
          records_for_page.clear();
        }
        current_page = page_id;
      }
      records_for_page.emplace_back(std::make_pair(it.key(), it.value()));
    }
    // ThreadPool destructor waits for work to finish
  }
  buf_mgr_->FlushDirty();
  mtable_.reset(new MemTable()); 
  return Status::OK();
}

void DBImpl::FlushWorkerMain(
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

}  // namespace llsm
