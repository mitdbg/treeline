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

namespace {

void PinToCore(uint32_t core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t thread = pthread_self();
  if (pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset) < 0) {
    throw std::runtime_error("Error pinning thread to core " +
                             std::to_string(core_id));
  }
}

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
    PinToCore(id);
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

static constexpr double kFillPct = 0.5;
static constexpr size_t kRecordSize = 16;

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
    : options_(std::move(options)), db_path_(std::move(db_path)) {}

Status DBImpl::Initialize() {
  // Create directory and an appropriate number of files (segments), one per
  // worker thread.
  if (mkdir(db_path_.c_str(), 0755) != 0) {
    return Status::IOError("Failed to create new directory:", db_path_);
  }
  segments_ = options_.num_flush_threads;

  // Compute records per page to achieve kFillPct
  uint32_t records_per_page = Page::kSize * 0.9 * kFillPct / kRecordSize;

  total_pages_ = options_.num_keys / records_per_page;
  if (options_.num_keys % records_per_page != 0) ++total_pages_;

  pages_per_segment_ = total_pages_ / segments_;
  if (total_pages_ % segments_ != 0) ++pages_per_segment_;

  // Initialize buffer manager
  BufMgrOptions buf_mgr_options;
  buf_mgr_options.num_files = segments_;
  buf_mgr_options.page_size = Page::kSize;
  buf_mgr_options.pages_per_segment_ = pages_per_segment_;
  buf_mgr_ = std::make_unique<BufferManager>(buf_mgr_options, db_path_);

  // Preallocate the pages with the key space
  uint64_t lower_key = 0;
  uint64_t upper_key =
      records_per_page;  // TODO - Linearly space the input records
  for (unsigned page_id = 0; page_id < total_pages_; page_id++) {
    uint64_t swapped_lower = __builtin_bswap64(lower_key);
    uint64_t swapped_upper = __builtin_bswap64(upper_key);
    auto& bf = buf_mgr_->FixPage(page_id, /*exclusive = */ true);
    Page page(reinterpret_cast<void*>(bf.GetPage()),
              Slice(reinterpret_cast<const char*>(&swapped_lower), 8),
              Slice(reinterpret_cast<const char*>(&swapped_upper), 8));
    buf_mgr_->UnfixPage(bf, /*is_dirty = */ true);

    lower_key += records_per_page;
    upper_key += records_per_page;
  }
  buf_mgr_->FlushDirty();
  last_key_ = lower_key;
  return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key,
                   const Slice& value) {
  return mtable_.Put(key, value);
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value_out) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return Status::NotSupported("Unimplemented.");
}

Status DBImpl::FlushMemTable() { // FIXME: Make MemTable iterator work with range for
  /*PinToCore(0);
  ThreadPool workers(options_.num_flush_threads);
  uint32_t current_page = UINT32_MAX;
  std::vector<std::pair<const Slice*, const Slice*>> records_for_page;
  for (const auto& kv : mtable_) {
    uint64_t raw_key = *reinterpret_cast<const uint64_t*>(kv.first.data());
    double rel_pos =
        (double)__builtin_bswap64(raw_key) / (double)options_.num_keys;
    uint32_t page_id = rel_pos * total_pages_;
    // The memtable is in sorted order - once we "pass" a page, we won't return
    // to it
    if (page_id != current_page) {
      if (current_page != UINT32_MAX) {
        // Submit flush job to workers - this is not the "first" page
        workers.Submit([records_for_page(std::move(records_for_page)), this,
                        current_page]() {
          ThreadFlushMain2(records_for_page, current_page);
        });
        records_for_page.clear();
      }
      current_page = page_id;
    }
    records_for_page.emplace_back(std::make_pair(&kv.first, &kv.second.first));
  }
  // ThreadPool destructor waits for work to finish*/
  return Status::OK();
}

void DBImpl::ThreadFlushMain2(
    const std::vector<std::pair<const Slice*, const Slice*>>& records,
    size_t page_id) {
  
  auto& bf = buf_mgr_->FixPage(page_id, /*exclusive = */ true);
  Page* page = bf.GetPage();
  
  for (const auto& kv : records) {
    auto s = page->Put(*kv.first, *kv.second);
    assert(s.ok());
  }
  buf_mgr_->UnfixPage(bf, /*is_dirty = */ true);
}

}  // namespace llsm
