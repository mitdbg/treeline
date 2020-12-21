// Acknowledgement: This API was adapted from LevelDB, and so we reproduce the
// LevelDB copyright statement below.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdlib>

namespace llsm {

// Database options
struct Options {
  // Create the database directory if it does not exist
  bool create_if_missing = false;

  // If set, prevent the database from being opened if it already exists
  bool error_if_exists = false;

  uint64_t num_keys = 5000000;
  unsigned num_flush_threads = 4;
};

struct ReadOptions {};

struct WriteOptions {};

struct BufMgrOptions {
  size_t buffer_manager_size = 16384;
  size_t page_size = 4096; 
  size_t alignment = 512; // Required by O_DIRECT

  // Intended for the file manager
  size_t use_direct_io = false;
  size_t num_files = 1;
  size_t pages_per_segment_ = 0;
  size_t growth_pages = 256;
};

}  // namespace llsm
