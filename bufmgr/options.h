#pragma once

#include <cstdlib>
#include "model/model.h"

namespace llsm { 

struct BufMgrOptions {
  size_t buffer_manager_size = 16384;
  size_t page_size = 4096; 
  size_t alignment = 512; // Required by O_DIRECT

  // Intended for the file manager
  bool use_direct_io = false;
  size_t num_segments = 1;
  size_t pages_per_segment = 16384;
  size_t growth_pages = 256;
  size_t total_pages = 16384;
};

} // namespace llsm
