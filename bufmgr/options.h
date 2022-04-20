#pragma once

#include <cstdlib>

#include "db/page.h"
#include "treeline/options.h"

namespace tl {

// Configuration options used by the `BufferManager`.
struct BufMgrOptions {
  // Create `BufMgrOptions` using its default values (defined below).
  BufMgrOptions() {}

  // Sets the options that are identical in both `Options` and `BufMgrOptions`
  // using an existing `Options` instance. All other options are set to their
  // default values (defined below).
  explicit BufMgrOptions(const Options& options)
      : buffer_pool_size(options.buffer_pool_size),
        use_direct_io(options.use_direct_io) {}

  // The size of the buffer pool, in bytes.
  size_t buffer_pool_size = 64 * 1024 * 1024;

  // The number of segments to use to store the pages. The pages are equally
  // divided among all the segments.
  size_t num_segments = 1;

  // Whether or not the buffer manager should use direct I/O.
  bool use_direct_io = false;

  // Whether the buffer manager should be run without an underlying file manager, 
  // for performance benchmarking purposes.
  bool simulation_mode = false;
};

}  // namespace tl
