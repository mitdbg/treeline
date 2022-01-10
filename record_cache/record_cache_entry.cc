#include "record_cache_entry.h"

#include "assert.h"

namespace llsm {

const uint8_t RecordCacheEntry::kValidMask = 0x80;      // 1000 0000
const uint8_t RecordCacheEntry::kDirtyMask = 0x40;      // 0100 0000
const uint8_t RecordCacheEntry::kWriteTypeMask = 0x20;  // 0010 0000
const uint8_t RecordCacheEntry::kPriorityMask = 0x07;   // 0000 0111

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

bool RecordCacheEntry::SetPriorityTo(uint8_t priority) {
  if ((priority & ~kPriorityMask) != 0) {
    metadata_ |= kPriorityMask;  // Just set it to maximum.
    return false;
  }
  metadata_ &= ~kPriorityMask;
  metadata_ |= priority;
  return true;
}
uint8_t RecordCacheEntry::GetPriority() { return (metadata_ & kPriorityMask); }

uint8_t RecordCacheEntry::IncrementPriority(bool return_post) {
  uint8_t old_priority = GetPriority();
  if (old_priority != kPriorityMask) ++metadata_;
  if (return_post) {
    return GetPriority();
  } else {
    return old_priority;
  }
}

uint8_t RecordCacheEntry::DecrementPriority(bool return_post) {
  uint8_t old_priority = GetPriority();
  if (old_priority != 0) --metadata_;
  if (return_post) {
    return GetPriority();
  } else {
    return old_priority;
  }
}

Slice RecordCacheEntry::GetKey() const { return key_; }
void RecordCacheEntry::SetKey(Slice key) { key_ = key; }

Slice RecordCacheEntry::GetValue() const { return value_; }
void RecordCacheEntry::SetValue(Slice value) { value_ = value; }

}  // namespace llsm