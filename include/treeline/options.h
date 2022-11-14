// Acknowledgement: This API was adapted from LevelDB, and so we reproduce the
// LevelDB copyright statement below.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>

namespace tl {

// Options used to inform the database about the key space (the distribution is
// assumed to be uniform).
struct KeyDistHints {
  size_t num_keys = 5000000;
  uint64_t min_key = 0;
  uint64_t key_step_size = 1;
  size_t record_size = 16;  // Including `key_size`.
  size_t key_size = 8;

  // How full each database page should be, as a value between 1 and 100
  // inclusive (representing a percentage).
  uint32_t page_fill_pct = 50;

  // Returns the number of records that should be placed in a page based on the
  // values of `page_fill_pct`, `record_size`, and the database's page size
  // (internally represented by the `Page::kSize` constant).
  size_t records_per_page() const;

  // Returns the number of required pages based on the values of `num_keys` and
  // using records_per_page().
  size_t num_pages() const;
};

// Database options
struct Options {
  // Create the database directory if it does not exist
  bool create_if_missing = true;

  // If set, prevent the database from being opened if it already exists
  bool error_if_exists = false;

  // Use direct I/O when writing to/reading from database files
  bool use_direct_io = false;

  // The maximum size of TreeLine's buffer pool, in bytes.
  size_t buffer_pool_size = 64 * 1024 * 1024;

  // The maximum size of a memtable before it should be flushed to persistent
  // storage, in bytes.
  size_t memtable_flush_threshold = 64 * 1024 * 1024;

  // The minimum size in bytes of a memtable batch associated with a certain
  // page necessary to actually copy the entries out during a memtable flush.
  size_t deferred_io_batch_size = 1;

  // The maximum number of times that we are allowed to not copy some page out
  // during a memtable flush.
  uint64_t deferred_io_max_deferrals = 1;

  // Currently only used when creating a new database. When reopening an
  // existing database, these values are ignored.
  KeyDistHints key_hints;

  // The number of background threads TreeLine should use (must be at least 2).
  // TreeLine uses one background thread to coordinate flushing the memtable and
  // needs at least one other background thread to run the flush work.
  unsigned background_threads = 4;

  // If true, TreeLine will pin the background threads to cores `0` to
  // `background_threads - 1`.
  bool pin_threads = true;

  // If true, TreeLine will try to optimize the FlushOptions for every flush.
  // In this case, `batch_scaling_factor` will be used to calculate optimal
  // deferred I/O parameters.
  bool deferral_autotuning = false;
  double batch_scaling_factor = 1;

  // If true, TreeLine will try to optimize the memory allocation between the
  // buffer pool and the memtables, keeping the total memory budget to
  // buffer_pool_size
  // + 2 * memtable_flush_threshold.
  bool memory_autotuning = false;

  // The minimum length of an overflow chain for which reorganization is
  // triggered.
  size_t reorg_length = 5;

  // If true, TreeLine will print messages to a debug log.
  bool enable_debug_log = true;

  // The maximum number of pages that reorganizing a single chain can produce.
  size_t max_reorg_fanout = 50;

  // The capacity of the record cache in records.
  size_t record_cache_capacity = 1024 * 1024;

  // Optimistically cache, with a lower priority, all records on the same page
  // as a record requested by the user.
  bool optimistic_caching = false;

  // If true, the record cache will try to batch writes for the same page when
  // writing out a dirty entry.
  bool rec_cache_batch_writeout = true;

  // Whether the record cache should use the LRU eviction policy.
  bool rec_cache_use_lru = false;
};

struct ReadOptions {};

struct WriteOptions {
  // If true, TreeLine will optimize inserts for bulk loading sorted keys.
  bool sorted_load = false;
  // Only checked when `sorted_load` is true. If true, the inserted values will
  // be checked to ensure they are indeed sorted. If not, the user is
  // responsible for ensuring that.
  bool perform_checks = true;
  // Only checked when `sorted_load` is true. If true, the loaded records will
  // be flushed to disk by the buffer manager after the bulk load is complete.
  bool flush_dirty_after_bulk = true;

  // If true, the write will not be written to TreeLine's write-ahead log.
  //
  // If the database process crashes shortly after a write with
  // `bypass_wal == false` is made, the write may be lost.
  bool bypass_wal = false;

  // If true, the write will be flushed from the operating system page cache
  // before the write is considered complete. If this flag is true, writes will
  // be slower.
  //
  // If this flag is false, and the machine crashes, some recent writes may be
  // lost. Note that if it is just the process that crashes (i.e., the machine
  // does not reboot), no writes will be lost even if this flag is false.
  //
  // In other words, a write with `sync == false` has similar crash semantics as
  // the `write()` system call. A write with `sync == true` has similar crash
  // semantics to a `write()` system call followed by `fdatasync()`.
  //
  // NOTE: This flag only has an effect when `bypass_wal` is false.
  bool sync = false;
};

}  // namespace tl
