#include "record_cache.h"

#include "llsm/pg_stats.h"

namespace llsm {

std::vector<RecordCacheEntry> RecordCache::cache_entries{};

RecordCache::RecordCache(const uint64_t capacity, WriteOutFn write_out,
                         KeyBoundsFn key_bounds)
    : capacity_(capacity),
      clock_(0),
      write_out_(std::move(write_out)),
      key_bounds_(std::move(key_bounds)) {
  tree_ = std::make_unique<ART_OLC::Tree>(TIDToARTKey);
  cache_entries.resize(capacity_);
}

RecordCache::~RecordCache() {
  for (auto i = 0; i < capacity_; ++i) {
    cache_entries[i].Lock(/*exclusive = */ false);
    WriteOutIfDirty(i);
    cache_entries[i].Unlock();
  }
  tree_.reset();  // Delete ART before freeing anything, so that entries are
                  // inaccessible while freeing.
  for (auto i = 0; i < capacity_; ++i) {
    FreeIfValid(i);
  }
  cache_entries.clear();
}

Status RecordCache::Put(const Slice& key, const Slice& value, bool is_dirty,
                        format::WriteType write_type, uint8_t priority,
                        bool safe) {
retry:
  uint64_t index;
#ifndef NDEBUG
  bool found;
  if (!override) {
    found = GetCacheIndex(key, /*exclusive = */ true, &index, safe).ok();
  } else {
    found = false;
    override = false;
  }
#else
  bool found = GetCacheIndex(key, /*exclusive = */ true, &index, safe).ok();
#endif
  char* ptr = nullptr;
  RecordCacheEntry* entry = nullptr;

  // If this key is not cached, need to make room by evicting first.
  if (!found) {
    index = SelectForEviction();
    entry = &cache_entries[index];
    if (safe) entry->Lock(/*exclusive = */ true);
    if (entry->IsValid()) {
      if (entry->IsDirty()) {
        pg::PageGroupedDBStats::Local().BumpCacheDirtyEvictions();
      } else {
        pg::PageGroupedDBStats::Local().BumpCacheCleanEvictions();
      }
      WriteOutIfDirty(index);
      Key art_key;
      TIDToARTKey(index + 1, art_key);
      auto t1 = tree_->getThreadInfo();
      tree_->remove(art_key, index + 1, t1);
    }
  } else {
    entry = &cache_entries[index];
  }

  // If this key is already cached, it is already at least as fresh as any
  // non-dirty copy we might try to overwrite it with.
  if (found && !is_dirty) {
    if (safe) entry->Unlock();
    return Status::OK();
  }

  // Do we need to allocate memory? Only if record is newly-cached, or if the
  // new value is larger.
  if (found && entry->GetValue().size() >= value.size()) {
    ptr = const_cast<char*>(entry->GetKey().data());
  } else {
    FreeIfValid(index);
    ptr = static_cast<char*>(malloc(key.size() + value.size()));

    // Update key.
    memcpy(ptr, key.data(), key.size());
    entry->SetKey(Slice(ptr, key.size()));
  }

  // Update value.
  memcpy(ptr + key.size(), value.data(), value.size());
  entry->SetValue(Slice(ptr + key.size(), value.size()));

  // Update metadata.
  entry->SetValidTo(true);
  entry->SetDirtyTo(found ? (is_dirty || entry->IsDirty()) : (is_dirty));
  if (is_dirty) entry->SetWriteType(write_type);
  entry->SetPriorityTo(priority);

  // Update ART.
  if (!found) {
    Key art_key;
    TIDToARTKey(index + 1, art_key);
    auto t2 = tree_->getThreadInfo();
    bool success = tree_->insert(art_key, index + 1, t2);

    if (!success) {  // Another thread cached the same key concurrently.
      // Set this cache entry up for eviction.
      entry->SetDirtyTo(false);
      entry->SetPriorityTo(0);
      if (safe) entry->Unlock();

      // If this was optimistic caching, having a more recent write cached is
      // "success".
      if (!is_dirty) return Status::OK();

      // Otherwise, we need to retry this write.
      goto retry;
    }
  }

  if (safe) entry->Unlock();

  return Status::OK();
}

Status RecordCache::PutFromRead(const Slice& key, const Slice& value,
                                uint8_t priority) {
  return Put(key, value, /*is_dirty = */ false,
             /*** ignored */ format::WriteType::kWrite /***/, priority);
}

Status RecordCache::GetCacheIndex(const Slice& key, bool exclusive,
                                  uint64_t* index_out, bool safe) const {
  Key art_key;
  SliceToARTKey(key, art_key);
  auto t = tree_->getThreadInfo();

  bool locked_successfully = false;
  TID tid;

  do {
    tid = tree_->lookup(art_key, t);
    if (tid == 0) {
      pg::PageGroupedDBStats::Local().BumpCacheMisses();
      return Status::NotFound("Key not in cache");
    }
    if (safe) locked_successfully = cache_entries[tid - 1].TryLock(exclusive);
  } while (!locked_successfully && safe);

  *index_out = tid - 1;
  cache_entries[*index_out].IncrementPriority();

  pg::PageGroupedDBStats::Local().BumpCacheHits();
  return Status::OK();
}

Status RecordCache::GetRange(const Slice& start_key, size_t num_records,
                             std::vector<uint64_t>* indices_out) const {
  Key art_key;
  SliceToARTKey(start_key, art_key);
  auto t = tree_->getThreadInfo();

  TID results_out[num_records];

  // Retrieve & lock in ART.
  size_t num_found = 0;
  tree_->lookupRange(art_key, results_out, num_records, num_found, t,
                     &cache_entries);

  // Place in vector.
  indices_out->resize(num_found);
  for (uint64_t i = 0; i < num_found; ++i) {
    indices_out->at(i) = results_out[i] - 1;
  }

  return Status::OK();
}

Status RecordCache::GetRange(const Slice& start_key, const Slice& end_key,
                             std::vector<uint64_t>* indices_out) const {
  return GetRangeImpl(start_key, end_key, indices_out);
}

Status RecordCache::GetRangeImpl(const Slice& start_key, const Slice& end_key,
                                 std::vector<uint64_t>* indices_out,
                                 std::optional<uint64_t> index_locked_already,
                                 uint64_t sub_scan_size) const {
  Key start_art_key;
  SliceToARTKey(start_key, start_art_key);
  auto t = tree_->getThreadInfo();

  TID results_out[sub_scan_size];

  // Retrieve & lock in ART.
  bool should_scan_more = true;
  while (should_scan_more) {
    size_t num_found = 0;

    // The next largest key after `sub_scan_size` keys will be returned in
    // `start_art_key`, setting up the next iteration.
    tree_->lookupRange(start_art_key, results_out, sub_scan_size, num_found, t,
                       &cache_entries, &start_art_key, index_locked_already);

    // Three conditions to scan more:
    // 1. Didn't return too few records (would happen if we hit the upper key
    // space bound).
    // 2. The point from which to continue is beyond the keys we saw.
    // 3. The point from which to continue is below the `end_key`.
    should_scan_more =
        (num_found == sub_scan_size) &&
        (cache_entries[results_out[sub_scan_size - 1] - 1].GetKey().compare(
             ARTKeyToSlice(start_art_key)) < 0) &&
        (ARTKeyToSlice(start_art_key).compare(end_key) < 0);

    // Place in vector.
    for (uint64_t i = 0; i < num_found; ++i) {
      auto index = results_out[i] - 1;
      auto entry = &cache_entries[index];
      if (should_scan_more || entry->GetKey().compare(end_key) < 0) {
        indices_out->emplace_back(index);
      } else {
        entry->Unlock();
      }
    }
  }
  return Status::OK();
}

uint64_t RecordCache::WriteOutDirty() {
  uint64_t count = 0;
  for (auto i = 0; i < capacity_; ++i) {
    if (!cache_entries[i].IsDirty()) continue;

    cache_entries[i].Lock(/*exclusive = */ false);
    count += WriteOutIfDirty(i);
    cache_entries[i].Unlock();
  }
  return count;
}

void RecordCache::TIDToARTKey(TID tid, Key& key) {
  const Slice& slice_key = cache_entries[tid - 1].GetKey();
  SliceToARTKey(slice_key, key);
}

void RecordCache::SliceToARTKey(const Slice& slice_key, Key& art_key) {
  art_key.set(slice_key.data(), slice_key.size());
}

Slice RecordCache::ARTKeyToSlice(const Key& art_key) {
  return Slice(reinterpret_cast<const char*>(&art_key[0]), art_key.getKeyLen());
}

uint64_t RecordCache::SelectForEviction() {
  uint64_t candidate;
  uint64_t local_clock;
  uint64_t lookahead = capacity_ < kDefaultEvictionLookahead
                           ? capacity_
                           : kDefaultEvictionLookahead;

  // Implement the CLOCK algorithm, but if the first eviction
  // candidate you find is dirty, scan ahead by `lookahead` in the hopes
  // of evicting a non-dirty one instead.
  while (true) {
    local_clock = (clock_++) % capacity_;
    if (cache_entries[local_clock].GetPriority() == 0) {
      candidate = local_clock;
      break;
    }
    cache_entries[local_clock].DecrementPriority();
  }

  if (cache_entries[candidate].IsDirty()) {
    for (auto i = 0; i < lookahead; i++) {
      local_clock = (clock_++) % capacity_;
      if ((cache_entries[local_clock].GetPriority() == 0) &&
          !cache_entries[local_clock].IsDirty()) {
        candidate = local_clock;
        break;
      }
      cache_entries[local_clock].DecrementPriority();
    }
  }

  return candidate;
}

uint64_t RecordCache::WriteOutIfDirty(uint64_t index) {
  // Do nothing if not dirty, or if using a standalone record cache.
  auto entry = &cache_entries[index];
  bool was_dirty = entry->IsDirty();
  if (!was_dirty) return 0;
  if (!write_out_) {
    // Skip the write out because a write out function was not provided.
    entry->SetDirtyTo(false);
    return was_dirty;
  }

  Slice key = entry->GetKey();

  std::vector<uint64_t> indices;
  WriteOutBatch batch;

  if (key_bounds_) {
    auto [_, upper_bound] = key_bounds_(llsm::key_utils::ExtractHead64(key));
    Status s =
        GetRangeImpl(key, key_utils::IntKeyAsSlice(upper_bound).as<Slice>(),
                     &indices, index, kDefaultWriteOutSubScan);
    for (auto& idx : indices) {
      entry = &cache_entries[idx];
      if (entry->IsDirty()) {
        batch.emplace_back(entry->GetKey(), entry->GetValue(),
                           entry->GetWriteType());
      }
    }
  } else {
    indices.push_back(index);
    batch.emplace_back(entry->GetKey(), entry->GetValue(),
                       entry->GetWriteType());
  }

  assert(write_out_);
  write_out_(batch);

  for (auto& idx : indices) {
    cache_entries[idx].SetDirtyTo(false);
    if (idx != index) cache_entries[idx].Unlock();
  }
  return batch.size();
}

bool RecordCache::FreeIfValid(uint64_t index) {
  if (cache_entries[index].IsValid()) {
    auto ptr = const_cast<char*>(cache_entries[index].GetKey().data());
    if (ptr != nullptr) {
      free(ptr);  // The value is stored contiguously in the same chunk.
      cache_entries[index].SetKey(Slice(nullptr, 0));
      cache_entries[index].SetValue(Slice(nullptr, 0));
    }
    return true;
  } else {
    return false;
  }
}

uint64_t RecordCache::ClearCache(bool write_out_dirty) {
  clock_ = 0;
  uint64_t count = 0;
  for (auto i = 0; i < capacity_; ++i) {
    cache_entries[i].SetPriorityTo(0);
    if (write_out_dirty) {
      cache_entries[i].Lock(/*exclusive = */ false);
      count += WriteOutIfDirty(i);
      cache_entries[i].Unlock();
    } else {
      cache_entries[i].SetDirtyTo(false);
    }
  }

  return count;
}

std::vector<std::pair<Slice, Slice>> RecordCache::ExtractDirty() {
  // NOTE: This method is not thread safe and cannot be called concurrently with
  // any other public method. So we do not take locks.
  std::vector<std::pair<Slice, Slice>> dirty_records;
  dirty_records.reserve(capacity_ * 0.75);  // Rough guess.
  for (uint64_t i = 0; i < capacity_; ++i) {
    if (!cache_entries[i].IsDirty()) {
      continue;
    }
    dirty_records.emplace_back(cache_entries[i].GetKey(), cache_entries[i].GetValue());
    cache_entries[i].SetDirtyTo(false);
  }
  return dirty_records;
}

}  // namespace llsm
