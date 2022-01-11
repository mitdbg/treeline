#include "record_cache.h"

namespace llsm {

std::vector<RecordCacheEntry> RecordCache::cache_entries_ =
    std::vector<RecordCacheEntry>();

RecordCache::RecordCache(uint64_t capacity) : capacity_(capacity) {
  tree_ = std::make_unique<ART_OLC::Tree>(TIDToARTKey);
  cache_entries_.resize(capacity_);
  clock_ = 0;
}

RecordCache::~RecordCache() {
  tree_.reset();  // Delete ART before freeing anything.
  for (auto i = 0; i < capacity_; ++i) {
    WriteOutIfDirty(i);
    FreeIfValid(i);
  }
  cache_entries_.clear();
}

Status RecordCache::Put(const Slice& key, const Slice& value, bool is_dirty,
                        format::WriteType write_type, uint8_t priority) {
  // Find entry and evict contents if necessary.
  uint64_t index = SelectForEviction();
  WriteOutIfDirty(index);
  Key art_key;
  TIDToARTKey(index + 1, art_key);
  auto t1 = tree_->getThreadInfo();
  tree_->remove(art_key, index + 1, t1);
  FreeIfValid(index);

  // Overwrite metadata.
  cache_entries_[index].SetValidTo(true);
  cache_entries_[index].SetDirtyTo(is_dirty);
  if (is_dirty) cache_entries_[index].SetWriteType(write_type);
  cache_entries_[index].SetPriorityTo(priority);

  // Overwrite record.
  char* ptr = static_cast<char*>(malloc(key.size() + value.size()));
  memcpy(ptr, key.data(), key.size());
  memcpy(ptr + key.size(), value.data(), value.size());
  cache_entries_[index].SetKey(Slice(ptr, key.size()));
  cache_entries_[index].SetValue(Slice(ptr + key.size(), value.size()));

  // Update ART
  TIDToARTKey(index + 1, art_key);
  auto t2 = tree_->getThreadInfo();
  tree_->insert(art_key, index + 1, t2);

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

Status RecordCache::GetIndex(const Slice& key, uint64_t* index_out) const {
  Key art_key;
  SliceToARTKey(key, art_key);
  auto t = tree_->getThreadInfo();
  TID tid = tree_->lookup(art_key, t);  

  if (tid == 0) return Status::NotFound("Key not in cache");

  *index_out = tid - 1;

  return Status::OK();  
}

void RecordCache::TIDToARTKey(TID tid, Key& key) {
  const Slice& slice_key = cache_entries_[tid - 1].GetKey();
  SliceToARTKey(slice_key, key);
}

void RecordCache::SliceToARTKey(const Slice& slice_key, Key& art_key) {
  art_key.set(slice_key.data(), slice_key.size());
}

uint64_t RecordCache::SelectForEviction() {
  uint64_t local_clock;

  while (true) {
    local_clock = (clock_++) % capacity_;
    uint8_t p = cache_entries_[local_clock].GetPriority();
    if (p == 0) break;
    cache_entries_[local_clock].SetPriorityTo(p - 1);
  }

  return local_clock;
}

bool RecordCache::WriteOutIfDirty(uint64_t index) {
  bool was_dirty = cache_entries_[index].IsDirty();

  // Writeout unimplemented - requires LLSM integration.

  return was_dirty;
}

bool RecordCache::FreeIfValid(uint64_t index) {
  if (cache_entries_[index].IsValid()) {
    // The value is stored contiguously in the same allocated chunk.
    free(const_cast<char*>(cache_entries_[index].GetKey().data()));
    return true;
  } else {
    return false;
  }
}

}  // namespace llsm
