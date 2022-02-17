#include "record_cache.h"

#include "llsm/pg_stats.h"

namespace llsm {

std::vector<RecordCacheEntry> RecordCache::cache_entries{};

RecordCache::RecordCache(const uint64_t capacity, WriteOutFn write_out)
    : capacity_(capacity), clock_(0), write_out_(std::move(write_out)) {
  tree_ = std::make_unique<ART_OLC::Tree>(TIDToARTKey);
  cache_entries.resize(capacity_);
}

RecordCache::~RecordCache() {
  tree_.reset();  // Delete ART before freeing anything, so that entries are
                  // inaccessible while freeing.
  for (auto i = 0; i < capacity_; ++i) {
    cache_entries[i].Lock(/*exclusive = */ false);
    WriteOutIfDirty(i);
    FreeIfValid(i);
    cache_entries[i].Unlock();
  }
  cache_entries.clear();
}

Status RecordCache::Put(const Slice& key, const Slice& value, bool is_dirty,
                        format::WriteType write_type, uint8_t priority,
                        bool safe) {
  uint64_t index;
  bool found = GetCacheIndex(key, /*exclusive = */ true, &index, safe).ok();
  char* ptr = nullptr;

  // If this key is not cached, need to make room by evicting first.
  if (!found) {
    index = SelectForEviction();
    if (safe) cache_entries[index].Lock(/*exclusive = */ true);
    if (cache_entries[index].IsValid()) {
      if (cache_entries[index].IsDirty()) {
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
  }

  // If this key is already cached, it is already at least as fresh as any
  // non-dirty copy we might try to overwrite it with.
  if (found && !is_dirty) {
    if (safe) cache_entries[index].Unlock();
    return Status::OK();
  }

  // Do we need to allocate memory? Only if record is newly-cached, or if the
  // new value is larger.
  if (found && cache_entries[index].GetValue().size() >= value.size()) {
    ptr = const_cast<char*>(cache_entries[index].GetKey().data());
  } else {
    FreeIfValid(index);
    ptr = static_cast<char*>(malloc(key.size() + value.size()));

    // Update key.
    memcpy(ptr, key.data(), key.size());
    cache_entries[index].SetKey(Slice(ptr, key.size()));
  }

  // Update value.
  memcpy(ptr + key.size(), value.data(), value.size());
  cache_entries[index].SetValue(Slice(ptr + key.size(), value.size()));

  // Update metadata.
  cache_entries[index].SetValidTo(true);
  cache_entries[index].SetDirtyTo(
      found ? (is_dirty || cache_entries[index].IsDirty()) : (is_dirty));
  if (is_dirty) cache_entries[index].SetWriteType(write_type);
  cache_entries[index].SetPriorityTo(priority);

  // Update ART.
  if (!found) {
    Key art_key;
    TIDToARTKey(index + 1, art_key);
    auto t2 = tree_->getThreadInfo();
    tree_->insert(art_key, index + 1, t2);
  }

  if (safe) cache_entries[index].Unlock();

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

uint64_t RecordCache::SelectForEviction() {
  uint64_t local_clock;
  uint64_t dirty_candidate;
  bool have_dirty_candidate = false;

  // Implement the CLOCK algorithm, but if the first eviction
  // candidate you find is dirty, go around once more in the hopes
  // of evicting a non-dirty one instead.
  while (true) {
    local_clock = (clock_++) % capacity_;
    auto entry = &cache_entries[local_clock];

    bool zero_priority = (entry->GetPriority() == 0);
    bool is_dirty = entry->IsDirty();
    bool is_candidate = (local_clock == dirty_candidate);

    // Case 1: 0 priority and clean -> evict.
    if (zero_priority && !is_dirty) {
      break;
    }
    // Case 2: 0 priority but dirty, don't have a dirty candidate -> set as
    // dirty candidate and go around.
    else if (zero_priority && is_dirty && !have_dirty_candidate) {
      have_dirty_candidate = true;
      dirty_candidate = local_clock;
    }
    // Case 3: Got back to dirty candidate and it still has priority 0 -> evict
    // this time.
    else if (zero_priority && have_dirty_candidate && is_candidate) {
      break;
    }
    // Case 4: Got back to dirty candidate but now it has higher priority ->
    // unmake candidate and continue.
    else if (!zero_priority && have_dirty_candidate && is_candidate) {
      have_dirty_candidate = false;
    }

    entry->DecrementPriority();
  }

  return local_clock;
}

bool RecordCache::WriteOutIfDirty(uint64_t index) {
  // Do nothing if not dirty, or if using a standalone record cache.
  auto entry = &cache_entries[index];
  bool was_dirty = entry->IsDirty();
  if (!was_dirty) return false;
  if (!write_out_) {
    // Skip the write out because a write out function was not provided.
    entry->SetDirtyTo(false);
    return was_dirty;
  }

  Slice key = entry->GetKey();
  Slice value = entry->GetValue();

  assert(write_out_);
  write_out_({{key, value, entry->GetWriteType()}});

  entry->SetDirtyTo(false);
  return was_dirty;
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

}  // namespace llsm
