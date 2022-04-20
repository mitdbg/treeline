#include "record_cache_entry.h"

#include "assert.h"

namespace tl {

const uint8_t RecordCacheEntry::kValidMask = 0x80;      // 1000 0000
const uint8_t RecordCacheEntry::kDirtyMask = 0x40;      // 0100 0000
const uint8_t RecordCacheEntry::kWriteTypeMask = 0x20;  // 0010 0000
const uint8_t RecordCacheEntry::kPriorityMask = 0x07;   // 0000 0111

RecordCacheEntry::RecordCacheEntry() : metadata_(0) {
  pthread_rwlock_init(&rwlock_, nullptr);
}

RecordCacheEntry::~RecordCacheEntry() { pthread_rwlock_destroy(&rwlock_); }

RecordCacheEntry::RecordCacheEntry(RecordCacheEntry&& other) noexcept {
  pthread_rwlock_init(&rwlock_, nullptr);
  metadata_ = other.metadata_.load();
  key_ = Slice(other.GetKey());
  value_ = Slice(other.GetValue());

  other.metadata_ = 0;
  other.SetKey(Slice(nullptr, 0));
  other.SetValue(Slice(nullptr, 0));
}

RecordCacheEntry& RecordCacheEntry::operator=(
    RecordCacheEntry&& other) noexcept {
  if (this != &other) {
    pthread_rwlock_destroy(&rwlock_);

    pthread_rwlock_init(&rwlock_, nullptr);
    metadata_ = other.metadata_.load();
    key_ = Slice(other.GetKey());
    value_ = Slice(other.GetValue());

    other.metadata_ = 0;
    other.SetKey(Slice(nullptr, 0));
    other.SetValue(Slice(nullptr, 0));
  }
  return *this;
}

void RecordCacheEntry::SetValidTo(bool val) {
  val ? (metadata_ |= kValidMask) : (metadata_ &= ~kValidMask);
}
bool RecordCacheEntry::IsValid() { return (metadata_ & kValidMask); }

void RecordCacheEntry::SetDirtyTo(bool val) {
  val ? (metadata_ |= kDirtyMask) : (metadata_ &= ~kDirtyMask);
}
bool RecordCacheEntry::IsDirty() { return (metadata_ & kDirtyMask); }

void RecordCacheEntry::SetWriteType(format::WriteType type) {
  if (type == static_cast<format::WriteType>(0)) {
    metadata_ &= ~kWriteTypeMask;
  } else {
    metadata_ |= kWriteTypeMask;
  }
}
format::WriteType RecordCacheEntry::GetWriteType() {
  return static_cast<format::WriteType>((metadata_ & kWriteTypeMask) != 0);
}
bool RecordCacheEntry::IsWrite() {
  return (GetWriteType() == format::WriteType::kWrite);
}
bool RecordCacheEntry::IsDelete() {
  return (GetWriteType() == format::WriteType::kDelete);
}

uint8_t RecordCacheEntry::GetPriority() { return ExtractPriority(metadata_); }

uint8_t RecordCacheEntry::ExtractPriority(uint8_t flags) {
  return (flags & kPriorityMask);
}

bool RecordCacheEntry::SetPriorityTo(uint8_t priority) {
  if ((priority & ~kPriorityMask) != 0) {
    metadata_ |= kPriorityMask;  // Just set it to maximum.
    return false;
  }

  uint8_t old_metadata = metadata_.load();
  uint8_t new_metadata;

  do {
    // For subsequent iterations, compare_exchange_weak() will have updated
    // `old_metadata` appropriately when failing.
    new_metadata = (old_metadata & ~kPriorityMask) | priority;
  } while (!metadata_.compare_exchange_weak(old_metadata, new_metadata));

  return true;
}

uint8_t RecordCacheEntry::IncrementPriority(bool return_post) {
  uint8_t old_metadata = metadata_.load();
  uint8_t old_priority;
  uint8_t new_metadata;

  do {
    // For subsequent iterations, compare_exchange_weak() will have updated
    // `old_metadata` appropriately when failing.
    old_priority = ExtractPriority(old_metadata);

    // Check if already max. If so, do not update, so ignore `return_post`.
    if (old_priority == kPriorityMask) return old_priority;

    new_metadata = old_metadata + 1;  // Priority is in the LSBs.
  } while (!metadata_.compare_exchange_weak(old_metadata, new_metadata));

  // If we get here, we actually performed an increment.
  if (return_post) {
    return old_priority + 1;
  } else {
    return old_priority;
  }
}

uint8_t RecordCacheEntry::DecrementPriority(bool return_post) {
  uint8_t old_metadata = metadata_.load();
  uint8_t old_priority;
  uint8_t new_metadata;

  do {
    // For subsequent iterations, compare_exchange_weak() will have updated
    // `old_metadata` appropriately when failing.
    old_priority = ExtractPriority(old_metadata);

    // Check if already min. If so, do not update, so ignore `return_post`.
    if (old_priority == 0) return old_priority;

    new_metadata = old_metadata - 1;  // Priority is in the LSBs.
  } while (!metadata_.compare_exchange_weak(old_metadata, new_metadata));

  // If we get here, we actually performed an decrement.
  if (return_post) {
    return old_priority - 1;
  } else {
    return old_priority;
  }
}

Slice RecordCacheEntry::GetKey() const { return key_; }
void RecordCacheEntry::SetKey(Slice key) { key_ = key; }

Slice RecordCacheEntry::GetValue() const { return value_; }
void RecordCacheEntry::SetValue(Slice value) { value_ = value; }

void RecordCacheEntry::Lock(const bool exclusive) {
  exclusive ? pthread_rwlock_wrlock(&rwlock_) : pthread_rwlock_rdlock(&rwlock_);
}
bool RecordCacheEntry::TryLock(const bool exclusive) {
  auto ret = exclusive ? pthread_rwlock_trywrlock(&rwlock_)
                       : pthread_rwlock_tryrdlock(&rwlock_);
  return (ret == 0);
}
void RecordCacheEntry::Unlock() { pthread_rwlock_unlock(&rwlock_); }

uint64_t RecordCacheEntry::FindIndexWithin(std::vector<RecordCacheEntry>* vec) {
  return ((reinterpret_cast<uint8_t*>(this) -
           reinterpret_cast<uint8_t*>(vec->data())) /
          sizeof(tl::RecordCacheEntry));
}

}  // namespace tl
