// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <filesystem>

#include "treeline/options.h"
#include "treeline/slice.h"
#include "treeline/status.h"
#include "wal/format.h"

namespace tl {
namespace wal {

// A helper class for appending entries to a write ahead log (WAL). Instances of
// this class are not meant to be created or used directly. Instead, a WAL
// manager is responsible for creating and writing to `Writer` instances.
//
// See wal/format.h for a description of the WAL's on-disk format.
class Writer {
 public:
  // Create a WAL writer that will append data to a file at `log_path`.
  explicit Writer(const std::filesystem::path& log_path);
  ~Writer();

  // Check whether the WAL file was created successfully.
  Status GetCreationStatus() const;

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  // Adds the given `payload` to the WAL. If `options.sync` is true,
  // `fdatasync()` will also be called to guarantee that the payload has been
  // written to persistent storage.
  Status AddEntry(const WriteOptions& options, const Slice& payload);

 private:
  Status EmitPhysicalRecord(RecordType type, const uint8_t* ptr, size_t length,
                            bool sync);
  Status AppendToLog(const uint8_t* data, size_t length);
  Status SyncLog();

  int fd_;
  Status creation_status_;

  size_t block_offset_;  // Current offset in block

  // crc32c values for all supported record types. These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace wal
}  // namespace tl
