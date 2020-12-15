#include "db_impl.h"

#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <unordered_map>
#include <thread>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <pthread.h>

#include "db/page.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      throw std::runtime_error(std::string(error));                          \
    }                                                                        \
  } while (0)

namespace {

void PinToCore(uint32_t core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t thread = pthread_self();
  if (pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset) < 0) {
    throw std::runtime_error("Error pinning thread to core " + std::to_string(core_id));
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

}

namespace llsm {

static constexpr double kFillPct = 0.5;
static constexpr size_t kRecordSize = 16;

class File {
 public:
  static constexpr size_t kBufSize = 4096;
  File(const std::string& name) : fd_(-1) {
    CHECK_ERROR(fd_ = open(name.c_str(), O_CREAT | O_RDWR | O_SYNC, S_IRUSR | S_IWUSR));
  }
  ~File() {
    close(fd_);
  }
  void ReadPage(size_t offset, void* data) const {
    CHECK_ERROR(pread(fd_, data, kBufSize, offset));
  }
  void WritePage(size_t offset, const void* data) const {
    CHECK_ERROR(pwrite(fd_, data, kBufSize, offset));
  }
  void Sync() const {
    fsync(fd_);
  }
 private:
  int fd_;
};

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
  if (mkdir(db_path_.c_str(), 0755) != 0) {
    return Status::IOError("Failed to create new directory:", db_path_);
  }
  segments_ = options_.num_flush_threads;
  for (unsigned i = 0; i < segments_; i++) {
    files_.push_back(std::make_unique<File>(db_path_ + "/segment-" + std::to_string(i)));
  }
  // Compute records per page to achieve kFillPct
  uint32_t records_per_page = Page::kSize * 0.9 * kFillPct / kRecordSize;
  total_pages_ = options_.num_keys / records_per_page;
  if (options_.num_keys % records_per_page != 0) {
    total_pages_ += 1;
  }
  pages_per_segment_ = total_pages_ / segments_;
  if (total_pages_ % segments_ != 0) {
    pages_per_segment_ += 1;
  }
  // Preallocate the pages with the key space
  uint64_t lower_key = 0;
  uint64_t upper_key = records_per_page; // TODO - Linearly space the input records
  for (unsigned page_id = 0; page_id < total_pages_; page_id++) {
    uint64_t swapped_lower = __builtin_bswap64(lower_key);
    uint64_t swapped_upper = __builtin_bswap64(upper_key);
    Page page(Slice(reinterpret_cast<const char*>(&swapped_lower), 8),
              Slice(reinterpret_cast<const char*>(&swapped_upper), 8));
    unsigned segment_id = page_id / pages_per_segment_;
    auto& file = files_[segment_id];
    file->WritePage((page_id % pages_per_segment_) * Page::kSize, page.Ptr());
    lower_key += records_per_page;
    upper_key += records_per_page;
  }
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

/*
Status DBImpl::FlushMemTable() {
  // Map records to page_ids
  PageMap page_map;
  uint32_t current_page = UINT32_MAX;
  for (const auto& kv : mtable_) {
    uint64_t raw_key = *reinterpret_cast<const uint64_t*>(kv.first.data());
    double rel_pos = (double) __builtin_bswap64(raw_key) / (double) options_.num_keys;
    uint32_t page_id = rel_pos * total_pages_;
    // The memtable is in sorted order - once we "pass" a page, we won't return to it
    if (page_id != current_page) {
      page_map.emplace_back(std::make_pair(page_id, std::vector<std::pair<const Slice*, const Slice*>>()));
      current_page = page_id;
    }
    auto& page_entries = page_map.back();
    page_entries.second.emplace_back(std::make_pair(&kv.first, &kv.second.first));
  }

  std::vector<std::thread> workers;
  workers.reserve(options_.num_flush_threads);
  size_t pages_to_flush = page_map.size();
  size_t pages_per_thread = pages_to_flush / options_.num_flush_threads;
  size_t pages_remainder = pages_to_flush % options_.num_flush_threads;

  size_t map_offset = 0;
  for (unsigned i = 0; i < options_.num_flush_threads; i++) {
    size_t num = pages_per_thread;
    if (i < pages_remainder) num += 1;
    if (num == 0) continue;
    workers.emplace_back(std::thread(&DBImpl::ThreadFlushMain, this, &page_map, map_offset, num));
    map_offset += num;
  }
  for (auto& worker : workers) {
    worker.join();
  }
  for (auto& file : files_) {
    file->Sync();
  }
  return Status::OK();
}
*/

Status DBImpl::FlushMemTable() {
  PinToCore(0);
  ThreadPool workers(options_.num_flush_threads);
  uint32_t current_page = UINT32_MAX;
  std::vector<std::pair<const Slice*, const Slice*>> records_for_page;
  for (const auto& kv : mtable_) {
    uint64_t raw_key = *reinterpret_cast<const uint64_t*>(kv.first.data());
    double rel_pos = (double) __builtin_bswap64(raw_key) / (double) options_.num_keys;
    uint32_t page_id = rel_pos * total_pages_;
    // The memtable is in sorted order - once we "pass" a page, we won't return to it
    if (page_id != current_page) {
      if (current_page != UINT32_MAX) {
        // Submit flush job to workers - this is not the "first" page
        workers.Submit([records_for_page(std::move(records_for_page)), this, current_page]() {
          ThreadFlushMain2(records_for_page, current_page);
        });
        records_for_page.clear();
      }
      current_page = page_id;
    }
    records_for_page.emplace_back(std::make_pair(&kv.first, &kv.second.first));
  }
  // ThreadPool destructor waits for work to finish
  return Status::OK();
}

void DBImpl::ThreadFlushMain(const PageMap* to_flush, size_t offset, size_t num) {
  Page page;
  for (unsigned i = offset; i < offset + num; i++) {
    auto& vec_pair = (*to_flush)[i];
    uint32_t page_id = vec_pair.first;
    unsigned segment_id = page_id / pages_per_segment_;
    auto& file = files_[segment_id];
    size_t file_offset = (page_id % pages_per_segment_) * Page::kSize;
    file->ReadPage(file_offset, page.Ptr());
    for (const auto& kv : vec_pair.second) {
      auto s = page.Put(*kv.first, *kv.second);
      assert(s.ok());
    }
    file->WritePage(file_offset, page.Ptr());
  }
}

void DBImpl::ThreadFlushMain2(const std::vector<std::pair<const Slice*, const Slice*>>& records, size_t page_id) {
  Page page;
  unsigned segment_id = page_id / pages_per_segment_;
  auto& file = files_[segment_id];
  size_t file_offset = (page_id % pages_per_segment_) * Page::kSize;
  file->ReadPage(file_offset, page.Ptr());
  for (const auto& kv : records) {
    auto s = page.Put(*kv.first, *kv.second);
    assert(s.ok());
  }
  file->WritePage(file_offset, page.Ptr());
}

}  // namespace llsm
