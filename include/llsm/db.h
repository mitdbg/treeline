// Acknowledgement: This API was adapted from LevelDB, and so we reproduce the
// LevelDB copyright statement below.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "llsm/options.h"
#include "llsm/slice.h"
#include "llsm/status.h"

namespace llsm {

// The public Learned LSM database interface, representing an embedded,
// persistent, and ordered key-value store.
//
// All methods can be called concurrently without requiring external mutual
// exclusion. All methods return an OK status on success, and a non-OK status
// if an error occurs.
class DB {
 public:
  // Open a database instance stored at `path`.
  //
  // If the open succeeds, `*db_out` will point to a DB instance and this method
  // will return an OK status. Otherwise the returned status will indicate the
  // error that occurred and `*db_out` will not be modified. Callers need to
  // delete the DB instance when they are done using it to close the database.
  static Status Open(const Options& options, const std::string& path,
                     DB** db_out);

  DB() = default;
  virtual ~DB() = default;

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  // Set the database entry for `key` to `value`.
  //
  // It is not an error if `key` already exists in the database; this method
  // will overwrite the value associated with that key.
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // Retrieve the value corresponding to `key` and store it in `value_out`.
  //
  // If the `key` does not exist, `value_out` will not be changed and a status
  // will be returned where `Status::IsNotFound()` evaluates to true.
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value_out) = 0;

  // Remove the database entry (if any) for `key`.
  //
  // It is not an error if `key` does not exist in the database; this method
  // will be an effective "no-op" in this case.
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
};

}  // namespace llsm
