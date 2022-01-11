#include <stdint.h>

#include "../db/format.h"
#include "../include/llsm/slice.h"

namespace llsm {

// Representation of fields within `metadata_`:
//  bit       7   |   6   |     5     |  4  3  | 2  1  0
//  field   valid | dirty | WriteType | unused | priority

struct RecordCacheEntry {
  static const uint8_t kValidMask;
  static const uint8_t kDirtyMask;
  static const uint8_t kWriteTypeMask;
  static const uint8_t kPriorityMask;

  uint8_t metadata_ = 0;  // Invalid by default.
  Slice key_;
  Slice value_;

  // Set/query the valid bit
  void SetValidTo(bool val);
  bool IsValid();

  // Set/query the dirty bit
  void SetDirtyTo(bool val);
  bool IsDirty();

  // Modify write type
  void SetWriteType(format::WriteType type);
  format::WriteType GetWriteType();
  bool IsWrite();
  bool IsDelete();

  // Modify priority
  bool SetPriorityTo(
      uint8_t priority);  // If `priority` exceeds the appropriate range, sets
                          // it to the legal maximum and returns false.
  uint8_t GetPriority();
  uint8_t IncrementPriority(bool return_post = true);
  uint8_t DecrementPriority(bool return_post = true);

  // Access/modify key
  Slice GetKey() const;
  void SetKey(Slice key);

  // Access/modify value
  Slice GetValue() const;
  void SetValue(Slice value);
};

}  // namespace llsm
