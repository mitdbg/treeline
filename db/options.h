#pragma once

#include <cstdlib>

namespace tl {

// Options used to instruct the database on how to handle memtable flushing and
// deferral.
struct MemTableOptions {
  // The size above which this memtable will be flushed, in bytes.
  size_t flush_threshold = 64 * 1024 * 1024;

  // The maximum number of past deferrals for which this memtable will 
  // remember the exact number of past deferrals.
  //
  // E.g. if set to 0, this memtable will lump all records inserted via
  //      deferrals together,
  //      if set to 1, this memtable will distinguish between records deferred
  //      once, but lump all records deferred at least twice together,
  //      etc.
  size_t deferral_granularity = 0;
};

struct FlushOptions {
  // Disable I/O deferral during a MemTable flush.
  bool disable_deferred_io = false;

  // The minimum size in bytes of a memtable batch associated with a certain page
  // necessary to actually copy the entries out during this memtable flush.
  size_t deferred_io_batch_size = 1;

  // The maximum number of prior deferrals that this flush will accomodate (i.e.
  // all pages deferred at least that many times will not be deferred again in
  // this flush).
  size_t deferred_io_max_deferrals = 0;
};

} // namespace tl
