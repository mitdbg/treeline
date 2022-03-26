#pragma once

#include <cstdlib>

namespace llsm {
namespace pg {

  struct InsertTrackerOptions {
  // The number of inserts in each InsertTracker epoch; the total elements of
  // the equi-depth histogram used for insert forecasting.
  size_t num_inserts_per_epoch = 10000;

  // The number of bins in the insert forecasitng histogram.
  size_t num_partitions = 10;

  // The size of the reservoir sample based on which the partition boundaries
  // are set at the beginning of each epoch.
  size_t sample_size = 1000;
  
  size_t random_seed = 42;
};

// Options used by the page-grouped database implementation.
struct PageGroupedDBOptions {
  // If set to false, no segments larger than 1 page will be created.
  //
  // Setting this flag to false emulates a page-chained DB with chain flattening
  // after one overflow page becomes full.
  bool use_segments = true;

  // By default, put 45 +/- (2 * 5) records into each page.
  size_t records_per_page_goal = 45;
  size_t records_per_page_delta = 5;

  // If set to true, will write out the segment sizes and models to a CSV file
  // for debug purposes.
  bool write_debug_info = true;

  // If set to true, direct I/O will be disabled and synchronous writes will
  // also be disabled. On machines with spare memory, this means that most I/O
  // will leverage the file system's block cache and writes cannot be
  // considered durable until the file is closed or fsync-ed.
  //
  // This flag is only meant to be set to true for the tests and when running
  // experiment setup code not related to the evaluation.
  bool use_memory_based_io = false;

  // If set to 0, no background threads will be used. The background threads are
  // only used to issue I/O in parallel when possible.
  size_t num_bg_threads = 16;

  // The number of neighboring segments to check (in each direction) when
  // performing a rewrite of a segment. If set to 0, only the segment that is
  // "full" will be rewritten.
  uint32_t rewrite_search_radius = 5;

  // The capacity of the record cache in records.
  size_t record_cache_capacity = 1024 * 1024;

  // Optimistically cache, with a lower priority, all records on the same page
  // as a record requested by the user.
  bool optimistic_caching = false;

  // If set to true, the record cache will not be used (all read/write requests
  // will incur I/O).
  bool bypass_cache = false;

  // If true, the record cache will try to batch writes for the same page when
  // writing out a dirty entry.
  bool rec_cache_batch_writeout = true;

  // If true, the DB will attempt to flush the dirty writes in the cache in
  // parallel when it shuts down.
  bool parallelize_final_flush = false;

  // Options for the insert tracker, which is used for insert forecasting.
  InsertTrackerOptions tracker;
};

struct WriteOptions {
  // Optional hint that this write is an update for a key that already exists in
  // the database. Correctness is unaffecterd even if this flag is set
  // incorrectly, but performance might be.
  bool is_update = false;
};

}  // namespace pg
}  // namespace llsm
