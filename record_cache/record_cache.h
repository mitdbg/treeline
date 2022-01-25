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
  static std::vector<RecordCacheEntry> cache_entries;

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
  //
  // Setting `safe = false` lets us switch to a thread-unsafe variant that does
  // not acquire locks. It is intended purely for performance benchmarking.
  Status Put(const Slice& key, const Slice& value, bool is_dirty = false,
             format::WriteType write_type = format::WriteType::kWrite,
             uint8_t priority = 4, bool safe = true);

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

  // Retrieve the index of the cache entry associated with `key`, if any, and
  // lock it for reading or writing based on `exclusive`. If an entry is found,
  // returns an OK status; otherwise, a Status::NotFound() is returned.
  //
  // Setting `safe = false` lets us switch to a thread-unsafe variant that does
  // not acquire locks upon lookup. It is intended purely for performance
  // benchmarking.
  Status GetCacheIndex(const Slice& key, bool exclusive, uint64_t* index_out,
                       bool safe = true) const;

 private:
  // Required by the ART constructor. Populates `key` with the key of the record
  // stored at `tid` - 1 within the record cache. See note below.
  static void TIDToARTKey(TID tid, Key& key);

  // Populates `art_key` with the record in `slice_key`.
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
  // CAUTION: This ART implementation uses the value 0 to denote lookup misses,
  // so any indexes referring to `cache_entries` are incremented by 1 to produce
  // the corresponding ART TID. E.g. To indicate that a record is stored at
  // index 3 in `cache_entries`, we would store the TID 4 in ART.
  std::unique_ptr<ART_OLC::Tree> tree_;
};

}  // namespace llsm
