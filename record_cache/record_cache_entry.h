#include <pthread.h>
#include <stdint.h>

#include "db/format.h"
#include "llsm/slice.h"

namespace llsm {

class RecordCacheEntry {
 public:
  RecordCacheEntry();
  ~RecordCacheEntry();

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
  uint8_t IncrementPriority(bool return_post = true);  // With upper bound.
  uint8_t DecrementPriority(bool return_post = true);  // With lower bound.

  // Access/modify key
  Slice GetKey() const;
  void SetKey(Slice key);

  // Access/modify value
  Slice GetValue() const;
  void SetValue(Slice value);

  // Lock/try to lock/unlock the current entry, possibly for exclusive access if
  // `exclusive` is true.
  void Lock(const bool exclusive);
  bool TryLock(const bool exclusive); // Returns true iff successful.
  void Unlock();

 private:
  // Bitmasks for the metadata field.
  static const uint8_t kValidMask;
  static const uint8_t kDirtyMask;
  static const uint8_t kWriteTypeMask;
  static const uint8_t kPriorityMask;

  // A read-write lock to be held when accessing this entry.
  pthread_rwlock_t rwlock_;

  // The key of the record stored in this entry.
  Slice key_;

  // The value of the record stored in this entry.
  Slice value_;

  // The metadata associated with this entry.
  //
  //  bit       7   |   6   |     5     |  4  3  | 2  1  0
  //  field   valid | dirty | WriteType | unused | priority
  uint8_t metadata_;
};

}  // namespace llsm
