#pragma once

#include <cstdlib>

namespace llsm { 

struct BufMgrOptions {
  size_t buffer_manager_size = 16384;
  size_t page_size = 4096; 
  size_t alignment = 512; // Required by O_DIRECT

  // Intended for the file manager
  bool use_direct_io = false;
  size_t num_files = 1;
  size_t pages_per_segment_ = 0;
  size_t growth_pages = 256;
};

} // namespace llsm