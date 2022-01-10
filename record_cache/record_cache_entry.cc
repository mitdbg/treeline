#include "assert.h"
#include "record_cache_entry.h"

namespace llsm {

const uint8_t RecordCacheEntry::kValidMask = 0x80;     // 1000 0000
const uint8_t RecordCacheEntry::kDirtyMask = 0x40;     // 0100 0000
const uint8_t RecordCacheEntry::kPriorityMask = 0x07;  // 0000 0111

void RecordCacheEntry::SetValid() { metadata_ |= kValidMask; }
void RecordCacheEntry::UnsetValid() { metadata_ &= ~kValidMask; }
bool RecordCacheEntry::IsValid() { return (metadata_ & kValidMask); }

void RecordCacheEntry::SetDirty() { metadata_ |= kDirtyMask; }
void RecordCacheEntry::UnsetDirty() { metadata_ &= ~kDirtyMask; }
bool RecordCacheEntry::IsDirty() { return (metadata_ & kDirtyMask); }

bool RecordCacheEntry::SetPriorityTo(uint8_t priority) {
  if ((priority & ~kPriorityMask) != 0) return false;
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

Slice RecordCacheEntry::GetKey() { return key_; }
void RecordCacheEntry::SetKey(Slice key) { key_ = key; }

Slice RecordCacheEntry::GetValue() { return value_; }
void RecordCacheEntry::SetValue(Slice value) { value_ = value; }

}  // namespace llsm