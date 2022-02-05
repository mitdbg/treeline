#pragma once

#include <atomic>
#include <memory>
#include <optional>

#include "art_olc/Tree.h"
#include "bufmgr/buffer_manager.h"
#include "db/overflow_chain.h"
#include "llsm/statistics.h"
#include "llsm/status.h"
#include "model/model.h"
#include "record_cache_entry.h"
#include "util/thread_pool.h"

namespace llsm {

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

  // Initializes a record cache in tandem with a database.
  //
  // The underlying database was configured according to `options`, it uses
  // `model` to determine the appropriate page for each key and `buf_mgr` to
  // bring in pages from disk, while using `workers` is a set of background
  // threads. The systems also collects statistics in `stats`.
  //
  // The last three arguments can optionally be omitted when using a standalone
  // RecordCache. In that case, no persistence guarantees are provided, and data
  // will be lost when exceeding the size of the record cache.
  RecordCache(Options* options, Statistics* stats,
              std::shared_ptr<Model> model = nullptr,
              std::shared_ptr<BufferManager> buf_mgr = nullptr,
              std::shared_ptr<ThreadPool> workers = nullptr);

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
             uint8_t priority = kDefaultPriority, bool safe = true,
             size_t reorg_length = kDefaultReorgLength,
             uint32_t page_fill_pct = kDefaultFillPct);

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
                       bool safe = true) const;

  // Retrieve an ascending range of at most `num_records` records, starting from
  // the smallest record whose key is greater than or equal to `start_key`. The
  // cache indices holding the records are return in `indices_out`, while the
  // actual number of retrieved records is in `num_found`.
  Status GetRange(const Slice& start_key, size_t num_records,
                  uint64_t indices_out[], size_t& num_found) const;

  // Writes out all dirty cache entries to the appropriate longer-term data
  // structure. If this process creates overflow chains of length at least
  // `reorg_length`, reorganization will be triggered, whereby new pages are
  // filled to `page_fill_pct`. Returns the number of dirty cache entries that
  // were written out.
  uint64_t WriteOutDirty(size_t reorg_length, uint32_t page_fill_pct);

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
  //
  // If this process creates overflow chains of length at least `reorg_length`,
  // reorganization will be triggered, whereby new pages are filled to
  // `page_fill_pct`.
  //
  // The caller should ensure that it owns the mutex for the entry in question
  // (at least in non-exclusive mode).
  bool WriteOutIfDirty(uint64_t index, size_t reorg_length,
                       uint32_t page_fill_pct);

  // Frees the cache-owned copy of the record stored in the cache entry at
  // `index`, if the entry is valid. Returns true if the entry was valid.
  bool FreeIfValid(uint64_t index);

  // The number of cache entries.
  const uint64_t capacity_;

  // The options of the unedlying database.
  Options* options_;

  // The statistics of the underlying database.
  Statistics* stats_;

  // The index of the next cache entry to be considered for eviction.
  std::atomic<uint64_t> clock_;

  // A pointer to the buffer manager of the  underlying system, used for
  // flushing dirty record cache entries.
  std::optional<std::shared_ptr<BufferManager>> buf_mgr_;

  // A pointer to the model of the underlying system, used for finding the right
  // page for flushing dirty record cache entries.
  std::optional<std::shared_ptr<Model>> model_;

  // A pointer to a set of background workers, used to perform reorganization.
  std::optional<std::shared_ptr<ThreadPool>> workers_;

  // An index for the cache, using ART with optimistic lock coupling from
  // https://github.com/flode/ARTSynchronized/tree/master/OptimisticLockCoupling.
  //
  // CAUTION: This ART implementation uses the value 0 to denote lookup
  // misses, so any indexes referring to `cache_entries` are incremented
  // by 1 to produce the corresponding ART TID. E.g. To indicate that a
  // record is stored at index 3 in `cache_entries`, we would store the
  // TID 4 in ART.
  std::unique_ptr<ART_OLC::Tree> tree_;
};

}  // namespace llsm
