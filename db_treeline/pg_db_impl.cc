#include "pg_db_impl.h"

#include <algorithm>
#include <cassert>
#include <functional>

#include "treeline/pg_stats.h"
#include "util/key.h"

namespace fs = std::filesystem;

namespace tl {
namespace pg {

static thread_local size_t thread_id_ = ([]() {
  return std::hash<std::thread::id>{}(std::this_thread::get_id());
})();

Status PageGroupedDB::Open(const PageGroupedDBOptions& options,
                           const std::filesystem::path& db_path,
                           PageGroupedDB** db_out) {
  // TODO: This open logic could be improved, but it is good enough for our
  // current use cases.
  if (std::filesystem::exists(db_path) &&
      std::filesystem::is_directory(db_path) &&
      !std::filesystem::is_empty(db_path)) {
    // Reopening an existing database.
    Manager mgr = Manager::Reopen(db_path, options);
    *db_out = new PageGroupedDBImpl(db_path, options, std::move(mgr));
  } else {
    // Opening a new database.
    *db_out = new PageGroupedDBImpl(db_path, options, std::optional<Manager>());
  }
  return Status::OK();
}

PageGroupedDBImpl::PageGroupedDBImpl(fs::path db_path,
                                     PageGroupedDBOptions options,
                                     std::optional<Manager> mgr)
    : db_path_(std::move(db_path)),
      options_(std::move(options)),
      mgr_(std::move(mgr)),
      cache_(options_.record_cache_capacity, options.rec_cache_use_lru,
             std::bind(&PageGroupedDBImpl::WriteBatch, this,
                       std::placeholders::_1),
             options_.rec_cache_batch_writeout
                 ? std::bind(&PageGroupedDBImpl::GetPageBoundsFor, this,
                             std::placeholders::_1)
                 : RecordCache::KeyBoundsFn()),
      tracker_(options_.forecasting.use_insert_forecasting
                   ? std::make_shared<InsertTracker>(
                         options_.forecasting.num_inserts_per_epoch,
                         options_.forecasting.num_partitions,
                         options_.forecasting.sample_size,
                         options_.forecasting.random_seed)
                   : nullptr) {
  if (mgr_.has_value()) mgr_->SetTracker(tracker_);
}

PageGroupedDBImpl::~PageGroupedDBImpl() {
  if (!mgr_.has_value()) return;

  // Record statistics before shutting down.
  mgr_->PostStats();
  PageGroupedDBStats::Local().SetCacheBytes(cache_.GetSizeFootprintEstimate());

  if (!options_.parallelize_final_flush || options_.bypass_cache) return;

  // When the destructor runs, no external threads should be running any methods
  // on this class. So it is safe invoke non-thread-safe methods here.
  const auto slice_records = cache_.ExtractDirty();
  std::vector<std::pair<Key, Slice>> records;
  records.reserve(slice_records.size());
  for (const auto& slice_rec : slice_records) {
    records.emplace_back(key_utils::ExtractHead64(slice_rec.first),
                         slice_rec.second);
  }
  std::sort(records.begin(), records.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  mgr_->PutBatchParallel(records);
}

Status PageGroupedDBImpl::BulkLoad(const std::vector<Record>& records) {
  if (mgr_.has_value()) {
    return Status::NotSupported("Cannot bulk load a non-empty DB.");
  }

  // Validate the input.
  if (records.empty()) {
    return Status::InvalidArgument("Cannot bulk load zero records.");
  }
  Key prev_key;
  for (size_t i = 0; i < records.size(); ++i) {
    const Key curr_key = records[i].first;
    if (curr_key == Manager::kMinReservedKey ||
        curr_key == Manager::kMaxReservedKey) {
      return Status::InvalidArgument(
          "Detected reserved keys in the records being bulk loaded.");
    }
    if (i > 0) {
      if (prev_key > curr_key) {
        return Status::InvalidArgument(
            "The records being bulk loaded must be sorted in ascending order.");
      }
      if (prev_key == curr_key) {
        return Status::InvalidArgument(
            "Detected a duplicate key during the bulk load. All keys must be "
            "unique.");
      }
    }
    prev_key = curr_key;
  }

  // Run the bulk load.
  mgr_ = Manager::LoadIntoNew(db_path_, records, options_);
  mgr_->SetTracker(tracker_);
  return Status::OK();
}

Status PageGroupedDBImpl::Put(const WriteOptions& options, const Key key,
                              const Slice& value) {
  if (!mgr_.has_value()) {
    return Status::NotSupported(
        "DB must be bulk loaded before any writes are allowed.");
  }
  cache_.GetMasstreePointer()->thread_init(thread_id_);
  if (key == Manager::kMinReservedKey || key == Manager::kMaxReservedKey) {
    return Status::InvalidArgument("Cannot Put() a reserved key.");
  }
  Status s;
  if (!options_.bypass_cache) {
    key_utils::IntKeyAsSlice key_slice(key);
    s = cache_.Put(key_slice.as<Slice>(), value, /*is_dirty=*/true,
                   format::WriteType::kWrite, RecordCache::kDefaultPriority,
                   /*safe=*/true);
  } else {
    s = mgr_->PutBatch({{key, value}});
  }

  // Track successful genuine inserts.
  if (tracker_ != nullptr && s.ok() && !options.is_update) tracker_->Add(key);

  return s;
}

Status PageGroupedDBImpl::Get(const Key key, std::string* value_out) {
  if (!mgr_.has_value()) return Status::NotFound("DB is empty.");
  cache_.GetMasstreePointer()->thread_init(thread_id_);
  if (key == Manager::kMinReservedKey || key == Manager::kMaxReservedKey) {
    return Status::NotFound("Reserved keys cannot be used.");
  }

  const key_utils::IntKeyAsSlice key_slice_helper(key);
  const Slice key_slice = key_slice_helper.as<Slice>();

  // 1. Search the record cache.
  if (!options_.bypass_cache) {
    uint64_t cache_index;
    const Status cache_status =
        cache_.GetCacheIndex(key_slice, /*exclusive=*/false, &cache_index);
    if (cache_status.ok()) {
      auto entry = &RecordCache::cache_entries[cache_index];
      if (entry->IsDelete()) {
        entry->Unlock();
        return Status::NotFound("Key not found.");
      }
      value_out->assign(entry->GetValue().data(), entry->GetValue().size());
      entry->Unlock();
      return cache_status;
    }
  }

  // 2. Go to disk. Cache the record if found.
  auto [status, pages] = mgr_->GetWithPages(key, value_out);
  if (!status.ok() || options_.bypass_cache) return status;

  cache_.PutFromRead(key_slice, Slice(*value_out),
                     RecordCache::kDefaultPriority);
  if (options_.optimistic_caching) {
    for (const auto& page : pages) {
      for (auto it = page.GetIterator(); it.Valid(); it.Next()) {
        cache_.PutFromRead(it.key(), it.value(),
                           RecordCache::kDefaultOptimisticPriority);
      }
    }
  }

  return status;
}

Status PageGroupedDBImpl::GetRange(
    const Key start_key, const size_t num_records,
    std::vector<std::pair<Key, std::string>>* results_out,
    bool use_experimental_prefetch) {
  if (!mgr_.has_value()) {
    results_out->clear();
    return Status::OK();
  }
  cache_.GetMasstreePointer()->thread_init(thread_id_);
  if (start_key == Manager::kMinReservedKey ||
      start_key == Manager::kMaxReservedKey) {
    return Status::InvalidArgument(
        "The scan start key is reserved and cannot be used.");
  }

  const key_utils::IntKeyAsSlice key_slice_helper(start_key);
  const Slice key_slice = key_slice_helper.as<Slice>();

  std::vector<std::pair<Key, std::string>> results;
  if (use_experimental_prefetch) {
    mgr_->ScanWithExperimentalPrefetching(start_key, num_records, &results);
  } else {
    mgr_->Scan(start_key, num_records, &results);
  }

  std::vector<uint64_t> indices;
  if (!options_.bypass_cache) {
    cache_.GetRange(key_slice, num_records, &indices);
  }

  // Merge the results while preferring records in the cache over records read
  // from disk when the keys are equal.
  results_out->reserve(std::min(num_records, results.size() + indices.size()));

  size_t records_left = num_records;
  auto cache_it = indices.begin();
  auto disk_it = results.begin();
  while (records_left > 0 && cache_it != indices.end() &&
         disk_it != results.end()) {
    auto& entry = RecordCache::cache_entries[*cache_it];
    const Key cache_record_key = key_utils::ExtractHead64(entry.GetKey());
    if (cache_record_key <= disk_it->first) {
      results_out->emplace_back(cache_record_key, entry.GetValue().ToString());
      entry.Unlock();
      ++cache_it;
      if (cache_record_key == disk_it->first) {
        ++disk_it;
      }
    } else {
      // disk_it->first < cache_record_key
      // Move the value to avoid an extra memory copy. We do not need to refer
      // to it again.
      results_out->emplace_back(disk_it->first, std::move(disk_it->second));
      ++disk_it;
    }
    --records_left;
  }
  while (records_left > 0 && cache_it != indices.end()) {
    auto& entry = RecordCache::cache_entries[*cache_it];
    const Key cache_record_key = key_utils::ExtractHead64(entry.GetKey());
    results_out->emplace_back(cache_record_key, entry.GetValue().ToString());
    entry.Unlock();
    ++cache_it;
    --records_left;
  }
  while (records_left > 0 && disk_it != results.end()) {
    results_out->emplace_back(disk_it->first, std::move(disk_it->second));
    ++disk_it;
    --records_left;
  }

  // Release any remaining locks on record cache entries.
  while (cache_it != indices.end()) {
    auto& entry = RecordCache::cache_entries[*cache_it];
    entry.Unlock();

    ++cache_it;
  }

  return Status::OK();
}

void PageGroupedDBImpl::WriteBatch(const WriteOutBatch& records) {
  assert(mgr_.has_value());
  std::vector<std::pair<Key, Slice>> reformatted;
  reformatted.resize(records.size());
  std::transform(records.begin(), records.end(), reformatted.begin(),
                 [](const auto& rec) {
                   const auto& [key, value, write_type] = rec;
                   // TODO: Deletes are not yet supported.
                   assert(write_type == format::WriteType::kWrite);
                   return std::make_pair(key_utils::ExtractHead64(key), value);
                 });
  std::sort(reformatted.begin(), reformatted.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  mgr_->PutBatch(reformatted);
}

std::pair<Key, Key> PageGroupedDBImpl::GetPageBoundsFor(Key key) {
  return mgr_->GetPageBoundsFor(key);
}

Status PageGroupedDBImpl::FlattenRange(const Key start_key, const Key end_key) {
  if (!options_.use_segments) {
    return Status::NotSupported(
        "FlattenRange() only implemented for segments.");
  }
  if (start_key > end_key) {
    return Status::InvalidArgument(
        "The start key cannot be greater than the end key.");
  }
  if (start_key == Manager::kMinReservedKey ||
      start_key == Manager::kMaxReservedKey ||
      end_key == Manager::kMinReservedKey) {
    return Status::InvalidArgument(
        "Cannot use a reserved key as the start key and cannot use the minimum "
        "reserved key as the end key.");
  }
  return mgr_->FlattenRange(start_key, end_key);
}

}  // namespace pg
}  // namespace tl
