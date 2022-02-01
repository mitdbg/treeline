#include "record_cache.h"

namespace llsm {

std::vector<RecordCacheEntry> RecordCache::cache_entries{};

RecordCache::RecordCache(uint64_t capacity, std::shared_ptr<Model> model,
                         std::shared_ptr<BufferManager> buf_mgr)
    : capacity_(capacity) {
  model_ = (model != nullptr) ? model : std::optional<std::shared_ptr<Model>>();
  buf_mgr_ = (buf_mgr != nullptr)
                 ? buf_mgr
                 : std::optional<std::shared_ptr<BufferManager>>();
  tree_ = std::make_unique<ART_OLC::Tree>(TIDToARTKey);
  cache_entries.resize(capacity_);
  clock_ = 0;
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

// TODO: when admitting keys from the pages, they should not override newer
// values for the same keys that are present in the record cache.
Status RecordCache::Put(const Slice& key, const Slice& value, bool is_dirty,
                        format::WriteType write_type, uint8_t priority,
                        bool safe) {
  uint64_t index;
  bool found = GetCacheIndex(key, /*exclusive = */ true, &index, safe).ok();
  char* ptr = nullptr;

  if (!found) {
    index = SelectForEviction();
    if (safe) cache_entries[index].Lock(/*exclusive = */ true);
    if (cache_entries[index].IsValid()) {
      WriteOutIfDirty(index);
      Key art_key;
      TIDToARTKey(index + 1, art_key);
      auto t1 = tree_->getThreadInfo();
      tree_->remove(art_key, index + 1, t1);
    }
  }

  if (found && cache_entries[index].GetValue().size() >= value.size()) {
    ptr = const_cast<char*>(cache_entries[index].GetKey().data());
  } else {  // New record, or new value is larger.
    FreeIfValid(index);
    ptr = static_cast<char*>(malloc(key.size() + value.size()));

    // Overwrite metadata.
    cache_entries[index].SetValidTo(true);
    cache_entries[index].SetDirtyTo(
        found ? (is_dirty || cache_entries[index].IsDirty()) : (is_dirty));
    if (is_dirty) cache_entries[index].SetWriteType(write_type);
    cache_entries[index].SetPriorityTo(priority);

    // Update key.
    memcpy(ptr, key.data(), key.size());
    cache_entries[index].SetKey(Slice(ptr, key.size()));
  }

  // Update value.
  memcpy(ptr + key.size(), value.data(), value.size());
  cache_entries[index].SetValue(Slice(ptr + key.size(), value.size()));

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

Status RecordCache::PutFromWrite(const Slice& key, const Slice& value,
                                 uint8_t priority) {
  return Put(key, value, /*is_dirty = */ true, format::WriteType::kWrite,
             priority);
}

Status RecordCache::PutFromRead(const Slice& key, const Slice& value,
                                uint8_t priority) {
  return Put(key, value, /*is_dirty = */ false,
             /*** ignored */ format::WriteType::kWrite /***/, priority);
}

Status RecordCache::PutFromDelete(const Slice& key, uint8_t priority) {
  return Put(key, Slice(), /*is_dirty = */ true, format::WriteType::kDelete,
             priority);
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
    if (tid == 0) return Status::NotFound("Key not in cache");
    if (safe) locked_successfully = cache_entries[tid - 1].TryLock(exclusive);
  } while (!locked_successfully && safe);

  *index_out = tid - 1;
  cache_entries[*index_out].IncrementPriority();

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

  while (true) {
    local_clock = (clock_++) % capacity_;
    if (cache_entries[local_clock].GetPriority() == 0) break;
    cache_entries[local_clock].DecrementPriority();
  }

  return local_clock;
}

bool RecordCache::WriteOutIfDirty(uint64_t index) {
  bool was_dirty = cache_entries[index].IsDirty();
  if (!buf_mgr_.has_value() || !model_.has_value()) return was_dirty;

  // Writeout unimplemented - requires LLSM integration.
  // Should consider synchronization with reorganization code.

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

// Iterator-related functions follow below.

RecordCache::Iterator RecordCache::GetIterator() const {
  return RecordCache::Iterator();
}

bool RecordCache::Iterator::Valid() const { return valid_; }

uint64_t RecordCache::Iterator::Index() const {
  assert(Valid());
  return 0;  // TODO
}

void RecordCache::Iterator::Next(bool exclusive) {
  assert(Valid());
  // TODO
}

void RecordCache::Iterator::Seek(const Slice& target, bool exclusive) {
  // TODO
  valid_ = true;
}

void RecordCache::Iterator::SeekToFirst(bool exclusive) {
  // TODO
  valid_ = true;
}

void RecordCache::Iterator::Close() {
  // TODO
  valid_ = false;
}

}  // namespace llsm
