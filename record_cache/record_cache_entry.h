#include <stdint.h>

#include "../include/llsm/slice.h"

namespace llsm {

// Representation of fields within `metadata_`:
//  bit       7   |   6   | 5  4  3 | 2  1  0
//  field   valid | dirty |  unused | priority

struct RecordCacheEntry {
  static const uint8_t kValidMask;
  static const uint8_t kDirtyMask;
  static const uint8_t kPriorityMask;

  uint8_t metadata_  = 0; // Invalid by default.
  Slice key_;
  Slice value_;

  // Set/unset/query the valid bit
  void SetValid();
  void UnsetValid();
  bool IsValid();

  // Set/unset/query the dirty bit
  void SetDirty();
  void UnsetDirty();
  bool IsDirty();

  // Modify priority
  bool SetPriorityTo(uint8_t priority);  // Returns true if successful.
  uint8_t GetPriority();
  uint8_t IncrementPriority(bool return_post = true);
  uint8_t DecrementPriority(bool return_post = true);

  // Modify key
  Slice GetKey();
  void SetKey(Slice key);
  
  // Modify value
  Slice GetValue();
  void SetValue(Slice value);
};

}  // namespace llsm