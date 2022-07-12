#pragma once

#include <filesystem>
#include <utility>
#include <vector>

#include "treeline/pg_options.h"
#include "treeline/slice.h"
#include "treeline/status.h"

namespace tl {
namespace pg {

using Key = tl::key_utils::KeyHead;
using Record = std::pair<Key, Slice>;

// The public page-grouped TreeLine database interface, representing
// an embedded, persistent, and ordered key-value store.
//
// All methods return an OK status on success, and a non-OK status if an error
// occurs. Concurrent access to the database is currently not supported.
//
// At most one `DB` instance should be used at any time in a single process.
//
// This implementation currently only supports unsigned integer keys up to 64
// bits. Key `0` and key `2^64 - 1` are reserved and should not be used.
class PageGroupedDB {
 public:
  // Open a database instance stored at `path`.
  //
  // If the open succeeds, `*db_out` will point to a `PageGroupedDB` instance
  // and this method will return an OK status. Otherwise the returned status
  // will indicate the error that occurred and `*db_out` will not be modified.
  // Callers need to delete the DB instance when they are done using it to close
  // the database.
  //
  // NOTE: A database should not be opened by more than one process at any time.
  static Status Open(const PageGroupedDBOptions& options,
                     const std::filesystem::path& path, PageGroupedDB** db_out);

  PageGroupedDB() = default;
  virtual ~PageGroupedDB() = default;

  PageGroupedDB(const PageGroupedDB&) = delete;
  PageGroupedDB& operator=(const PageGroupedDB&) = delete;

  // Efficiently add multiple `records` with distinct, sorted, same-sized keys
  // and payloads to an empty database.
  //
  // The caller is responsible for verifying that `records` are distinct and
  // sorted by key. Returns Status::NotSupported if the database is not
  // initially empty.
  virtual Status BulkLoad(const std::vector<Record>& records) = 0;

  // Set the database entry for `key` to `value`.
  //
  // It is not an error if `key` already exists in the database; this method
  // will overwrite the value associated with that key.
  virtual Status Put(const WriteOptions& options, const Key key,
                     const Slice& value) = 0;

  // Retrieve the value corresponding to `key` and store it in `value_out`.
  //
  // If the `key` does not exist, `value_out` will not be changed and a status
  // will be returned where `Status::IsNotFound()` evaluates to true.
  virtual Status Get(const Key key, std::string* value_out) = 0;

  // Retrieve an ascending range of at most `num_records` records, starting from
  // the smallest record whose key is greater than or equal to `start_key`.
  //
  // If `use_experimental_prefetch` is set to true, this operation will use
  // prefetching when performing I/O. Prefetching relies on background threads,
  // so `PageGroupedDBOptions::num_bg_threads` must be greater than 0. The
  // prefetching implementation is not thread-safe and thus cannot run
  // concurrently with write requests.
  virtual Status GetRange(const Key start_key, const size_t num_records,
                          std::vector<std::pair<Key, std::string>>* results_out,
                          bool use_experimental_prefetch = false) = 0;

  // Removes all overflow pages in the specified key range. The `end_key` is
  // exclusive.
  //
  // All segments that intersect the specified range that have an overflow page
  // will be rewritten (to remove the overflow pages). Any segments that
  // intersect the specified range and do not have an overflow page will not be
  // modified.
  //
  // This method only guarantees that overflow pages in the specified range that
  // exist at the time this method is called are removed. Any overflow pages
  // created in the specified range while this method runs (e.g., by concurrent
  // threads) may be removed, but are not guaranteed to be removed.
  //
  // This method is thread-safe. However for performance reasons it should
  // generally not be called on regions of the key space that are experiencing a
  // high rate of writes.
  virtual Status FlattenRange(
      const Key start_key = 1,
      const Key end_key = std::numeric_limits<Key>::max()) = 0;
};

}  // namespace pg
}  // namespace tl
