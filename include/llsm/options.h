// Acknowledgement: This API was adapted from LevelDB, and so we reproduce the
// LevelDB copyright statement below.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdlib>
#include <cstdint>

namespace llsm {

// Database options
struct Options {
  // Create the database directory if it does not exist
  bool create_if_missing = false;

  // If set, prevent the database from being opened if it already exists
  bool error_if_exists = false;

  // Use direct I/O when writing to/reading from database files
  bool use_direct_io = false;

  // The maximum size of LLSM's buffer pool, in bytes.
  size_t buffer_pool_size = 64 * 1024 * 1024;

  // Temporary options used to inform the database about the key space (the
  // distribution is assumed to be uniform).
  uint64_t num_keys = 5000000;
  uint64_t min_key = 0;
  uint64_t key_step_size = 1;
  size_t record_size = 16;

  // How full each database page should be, as a value between 1 and 100
  // inclusive (representing a percentage).
  uint32_t page_fill_pct = 50;

  // The number of worker threads to use when flushing in-memory writes to
  // persistent storage.
  unsigned num_flush_threads = 4;
};

struct ReadOptions {};

struct WriteOptions {};

}  // namespace llsm
