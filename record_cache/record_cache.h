#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>

#include "db/format.h"
#include "db/overflow_chain.h"
#include "record_cache_entry.h"
#include "third_party/masstree_wrapper/masstree_wrapper.h"
#include "treeline/statistics.h"
#include "treeline/status.h"
#include "util/hash_queue.h"
#include "util/key.h"

namespace tl {

using WriteOutBatch = std::vector<std::tuple<Slice, Slice, format::WriteType>>;

class RecordCache {
 private:
  static const size_t kDefaultReorgLength = 5;
  static const uint32_t kDefaultFillPct = 50;

 public:
  // Default priority when caching records that were genuinely requested .
  static const uint8_t kDefaultPriority = 4;

  // Default priority for records cached optimistically due to being near
  // genuinely requested records.
  static const uint8_t kDefaultOptimisticPriority = 1;

  // A collection of cached records.
  // Must be static to work with the ART implementation.
  static std::vector<RecordCacheEntry> cache_entries;

  // A function that should implement record write out functionality. We use
  // this to decouple the cache from the database's persistence logic.
  using WriteOutFn = std::function<void(const WriteOutBatch&)>;

  // A function that should return the lower (inclusive) and upper (exclusive)
  // bounds of the page that the argument should be placed on.
  using KeyBoundsFn =
      std::function<std::pair<key_utils::KeyHead, key_utils::KeyHead>(
          key_utils::KeyHead)>;

  // Initializes a record cache in tandem with a database. `capacity` is
  // measured in the number of records. Setting `use_lru` will use LRU as the
  // eviction policy instead of the clock-priority algorithm.
  //
  // The last two arguments can optionally be omitted when using a standalone
  // RecordCache. In that case, no persistence guarantees are provided, and data
  // will be lost when exceeding the size of the record cache.
  RecordCache(uint64_t capacity, bool use_lru = false,
              WriteOutFn write_out = WriteOutFn(),
              KeyBoundsFn key_bounds = KeyBoundsFn());

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
             uint8_t priority = kDefaultPriority, bool safe = true);

  // Cache the pair `key`-`value`, originating from a read. This is a
  // convenience method that calls `Put()` with `is_dirty` set to false and
  // `EntryType::kWrite`.
  Status PutFromRead(const Slice& key, const Slice& value,
                     uint8_t priority = kDefaultPriority);

  // Retrieve the index of the cache entry associated with `key`, if any, and
  // lock it for reading or writing based on `exclusive`. If an entry is found,
  // returns an OK status; otherwise, a Status::NotFound() is returned.
  //
  // Setting `safe = false` lets us switch to a thread-unsafe variant that does
  // not acquire locks upon lookup. It is intended purely for performance
  // benchmarking.
  Status GetCacheIndex(const Slice& key, bool exclusive, uint64_t* index_out,
                       bool safe = true);

  // Retrieve an ascending range of at most `num_records` records, starting from
  // the smallest record whose key is greater than or equal to `start_key`. The
  // cache indices holding the records are return in `indices_out`.
  Status GetRange(const Slice& start_key, size_t num_records,
                  std::vector<uint64_t>* indices_out) const;

  // Retrieve an ascending range of records, starting from the smallest record
  // whose key is greater than or equal to `start_key` and returning no records
  // with key larger than `end_key`. The cache indices holding the records are
  // return in `indices_out`.
  Status GetRange(const Slice& start_key, const Slice& end_key,
                  std::vector<uint64_t>* indices_out) const;

  // Writes out all dirty cache entries to the appropriate longer-term data
  // structure. Returns the number of dirty entries written out.
  uint64_t WriteOutDirty();

  // Clears the cache: any clean cache records are deleted and any dirty cached
  // records are optionally written out based on `write_out_dirty` and then also
  // deleted. The eviction clock is reset to 0. Returns the number of dirty
  // entries written out.
  uint64_t ClearCache(bool write_out_dirty = true);

  // Returns all the dirty records in the cache. The caller takes on the
  // responsibility to write the dirty records to stable storage (i.e., all the
  // returned records will be marked clean inside the cache).
  //
  // This method is NOT thread safe and cannot run concurrently with any other
  // public methods. The pointers inside the returned records are only valid
  // until the next call to a public method.
  std::vector<std::pair<Slice, Slice>> ExtractDirty();

  // Get an estimate of the cache's size footprint. The returned size is missing
  // the size of ART. This method is NOT thread safe and cannot run concurrently
  // with any other public methods.
  uint64_t GetSizeFootprintEstimate() const;

  // A pointer to the Masstree wrapper used by the cache.
  std::shared_ptr<MasstreeWrapper<RecordCacheEntry>> GetMasstreePointer();

 private:
  // Implements `GetRange` but adds private functionality to avoid locking a
  // specific cache entry (used during writeout).
  Status GetRangeImpl(
      const Slice& start_key, const Slice& end_key,
      std::vector<uint64_t>* indices_out,
      std::optional<uint64_t> index_locked_already = std::nullopt) const;

  // Selects a cache entry according to the chosen policy, and returns the
  // corresponding index into the `cache_entries` vector. If the entry
  // originally selected is dirty and `use_lru_` is not set, will look at the
  // next `kDefaultEvictionLookahead` entries to try to find a clean one.
  uint64_t SelectForEviction();
  const uint64_t kDefaultEvictionLookahead = 32;

  // Writes out the cache entry at `index`, if dirty, to the appropriate
  // longer-term data structure. If a write out takes place, also writes out
  // all other cached dirty entries that correspond to the same page.
  //
  // Returns the number of dirty entries written out.
  //
  // The caller should ensure that it owns the mutex for the entry in question
  // (at least in non-exclusive mode).
  uint64_t WriteOutIfDirty(uint64_t index);

  // Frees the cache-owned copy of the record stored in the cache entry at
  // `index`, if the entry is valid. Returns true if the entry was valid.
  bool FreeIfValid(uint64_t index);

  // The number of cache entries.
  const uint64_t capacity_;

  // Whether to use the LRU policy for cache eviction.
  const bool use_lru_;

#ifndef NDEBUG
  // Debugging flag
  bool override = false;
#endif

  // The options of the underlying database.
  Options* options_;

  // The statistics of the underlying database.
  Statistics* stats_;

  // The index of the next cache entry to be considered for eviction.
  std::atomic<uint64_t> clock_;

  // The function to run when the cache needs to write out records (e.g.,
  // because they need to be evicted). This member can be "empty", which
  // indicates that no persistence guarantees are provided (data will be lost
  // when exceeding the size of the record cache).
  WriteOutFn write_out_;

  // The function to run to determine the range of keys that correspond to the
  // same page as some key being written out from the cache. This member can be
  // empty, in which case the cache will not attempt to write out additional
  // records from the same page when writing out a dirty record.
  KeyBoundsFn key_bounds_;

  std::shared_ptr<MasstreeWrapper<RecordCacheEntry>> tree_;

  std::unique_ptr<HashQueue<uint64_t>> lru_queue_;
};

}  // namespace tl
