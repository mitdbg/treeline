#pragma once

#include <pthread.h>
#include <stdint.h>

#include <atomic>
#include <vector>

#include "db/format.h"
#include "treeline/slice.h"

namespace tl {

class RecordCacheEntry {
 public:
  RecordCacheEntry();
  ~RecordCacheEntry();

  // Copy constructor/assignment - not permitted.
  RecordCacheEntry(const RecordCacheEntry& other) = delete;
  RecordCacheEntry& operator=(const RecordCacheEntry& other) = delete;

  // Move contructor/assignment - required by std::vector.
  //
  // Move can be implemented safely because the vector of cache entries is only
  // resized during initialization, not during execution, when it might have
  // been accessed by multiple threads.
  RecordCacheEntry(RecordCacheEntry&& other) noexcept;
  RecordCacheEntry& operator=(RecordCacheEntry&& other) noexcept;

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
  // `return_post` controls whether the returned value is pre- or post- the
  // attempted in-(de-)crement.
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
  bool TryLock(const bool exclusive);  // Returns true iff successful.
  void Unlock();

  // Retrieves the index of a `RecordCacheEntry` within a vector `vec`.
  uint64_t FindIndexWithin(std::vector<RecordCacheEntry>* vec);

 private:
  // Bitmasks for the metadata field.
  static const uint8_t kValidMask;
  static const uint8_t kDirtyMask;
  static const uint8_t kWriteTypeMask;
  static const uint8_t kPriorityMask;

  // Extracts the priority from `flags`, assuming the same encoding is used as
  // the encoding of `metadata_`.
  uint8_t ExtractPriority(uint8_t flags);

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
  std::atomic<uint8_t> metadata_;
};

}  // namespace tl
