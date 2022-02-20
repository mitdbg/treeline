#include "pg_db_impl.h"

#include <algorithm>
#include <cassert>
#include <functional>

#include "util/key.h"

namespace fs = std::filesystem;

namespace llsm {
namespace pg {

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
      cache_(options.record_cache_capacity,
             std::bind(&PageGroupedDBImpl::WriteBatch, this,
                       std::placeholders::_1),
             std::bind(&PageGroupedDBImpl::GetPageBoundsFor, this,
                       std::placeholders::_1)) {}

Status PageGroupedDBImpl::BulkLoad(const std::vector<Record>& records) {
  if (mgr_.has_value()) {
    return Status::NotSupported("Cannot bulk load a non-empty DB.");
  }
  mgr_ = Manager::LoadIntoNew(db_path_, records, options_);
  return Status::OK();
}

Status PageGroupedDBImpl::Put(const Key key, const Slice& value) {
  if (!mgr_.has_value()) {
    return Status::NotSupported(
        "DB must be bulk loaded before any writes are allowed.");
  }
  key_utils::IntKeyAsSlice key_slice(key);
  return cache_.Put(key_slice.as<Slice>(), value, /*is_dirty=*/true,
                    format::WriteType::kWrite, RecordCache::kDefaultPriority,
                    /*safe=*/true);
}

Status PageGroupedDBImpl::Get(const Key key, std::string* value_out) {
  if (!mgr_.has_value()) return Status::NotFound("DB is empty.");

  const key_utils::IntKeyAsSlice key_slice_helper(key);
  const Slice key_slice = key_slice_helper.as<Slice>();

  // 1. Search the record cache.
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

  // 2. Go to disk. Cache the record if found.
  auto [status, pages] = mgr_->GetWithPages(key, value_out);
  if (!status.ok()) return status;

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
    std::vector<std::pair<Key, std::string>>* results_out) {
  if (!mgr_.has_value()) {
    results_out->clear();
    return Status::OK();
  }

  const key_utils::IntKeyAsSlice key_slice_helper(start_key);
  const Slice key_slice = key_slice_helper.as<Slice>();

  std::vector<std::pair<Key, std::string>> results;
  mgr_->Scan(start_key, num_records, &results);

  std::vector<uint64_t> indices;
  cache_.GetRange(key_slice, num_records, &indices);

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

}  // namespace pg
}  // namespace llsm
