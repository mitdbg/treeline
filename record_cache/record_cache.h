#include <atomic>
#include <memory>

#include "art_olc/Tree.h"
#include "llsm/status.h"
#include "record_cache_entry.h"

namespace llsm {

class RecordCache {
 public:
  // A collection of cached records.
  // Must be static to work with the ART implementation.
  static std::vector<RecordCacheEntry> cache_entries_;

  // Initializes a record cache that can hold `capacity` records.
  RecordCache(uint64_t capacity);

  // Destroys the record cache, after writing back any dirty records.
  ~RecordCache();

  // Inserts the `key`-`value` pair into the record cache. The cache itself will
  // own a copy of the record. If necessary, evicts another record to make
  // space.
  //
  // The flag `is_dirty` indicates whether the inserted tuple (the same `key`
  // with the same `value`) is not already present elsewhere in the system. If
  // `is_dirty` is true, `write_type` further specifies whether this is an
  // insert/update, or a delete.
  //
  // Also provided is the eviction `priority` of the tuple, i.e. the # of times
  // the record will be skipped by the CLOCK algorithm.
  Status Put(const Slice& key, const Slice& value, bool is_dirty = false,
             format::WriteType write_type = format::WriteType::kWrite,
             uint8_t priority = 4);

  // Cache the pair `key`-`value`, originating from a write. This is a
  // convenience method that calls `Put()` with `is_dirty` set to true and
  // `EntryType::kWrite`.
  Status PutFromWrite(const Slice& key, const Slice& value,
                      uint8_t priority = 4);

  // Cache the pair `key`-`value`, originating from a read. This is a
  // convenience method that calls `Put()` with `is_dirty` set to false and
  // `EntryType::kWrite`.
  Status PutFromRead(const Slice& key, const Slice& value,
                     uint8_t priority = 4);

  // Cache a delete of `key`. This is a convenience method that calls `Put()`
  // with `is_dirty` set to false and `EntryType::kDelete`.
  Status PutFromDelete(const Slice& key, uint8_t priority = 4);

  // Retrieve the index of the cache entry associated with `key`, if any. If an
  // entry is found, either method will return an OK status; otherwise, a
  // Status::NotFound() will be returned.
  Status GetIndex(const Slice& key, uint64_t* index_out) const;

 private:
  // Required by the ART constructor. Populates `key` with the key of the record
  // stored at `tid` - 1 within the record cache. See note below.
  static void TIDToARTKey(TID tid, Key& key);

  // Populates `key` with the record in `slice_key`.
  static void SliceToARTKey(const Slice& slice_key, Key& art_key);

  // Selects a cache entry according to the chosen policy, and returns the
  // corresponding index into the `cache_entries` vector.
  uint64_t SelectForEviction();

  // Writes out the cache entry at `index`, if dirty, to the appropriate
  // longer-term data structure. Returns true if the entry was dirty.
  bool WriteOutIfDirty(uint64_t index);

  // Frees the cache-owned copy of the record stored in the cache entry at
  // `index`, if the entry is valid. Returns true if the entry was valid.
  bool FreeIfValid(uint64_t index);

  // The number of cache entries.
  const uint64_t capacity_;

  // The index of the next cache entry to be considered for eviction.
  std::atomic<uint64_t> clock_;

  // An index for the cache, using ART with optimistic lock coupling from
  // https://github.com/flode/ARTSynchronized/tree/master/OptimisticLockCoupling.
  //
  // CAUTION: This implementation uses the value 0 to denote lookup misses, so
  // any indexes referring to `cache_entries_` are incremented by 1 to produce
  // the corresponding ART TID.
  std::unique_ptr<ART_OLC::Tree> tree_;
};

}  // namespace llsm