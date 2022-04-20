// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "treeline/slice.h"
#include "treeline/status.h"
#include "wal/format.h"

namespace tl {
namespace wal {

// A helper class for reading entries from a write ahead log (WAL). Instances of
// this class are not meant to be created or used directly. Instead, a WAL
// manager is responsible for creating and reading from `Reader` instances.
//
// See wal/format.h for a description of the WAL's on-disk format.
class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected. `size` is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return records from the log at `log_path`.
  //
  // If `reporter` is non-null, it is notified whenever some data is
  // dropped due to a detected corruption. `*reporter` must remain
  // live while this `Reader` is in use.
  //
  // If `checksum` is true, verify checksums if available.
  //
  // The `Reader` will start reading at the first record located at physical
  // position >= `initial_offset` within the file.
  Reader(const std::filesystem::path& log_path, Reporter* reporter,
         bool checksum, uint64_t initial_offset);

  // Check whether the WAL file was opened successfully.
  Status GetCreationStatus() const;

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  // Read the next log entry into `*entry`. Returns true if read
  // successfully, false if we hit end of the input. May use
  // `*scratch` as temporary storage. The contents filled in `*entry`
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to `*scratch`.
  bool ReadEntry(Slice* entry, std::string* scratch);

  // Returns the physical offset of the last record returned by `ReadRecord()`.
  //
  // Undefined before the first call to `ReadRecord()`.
  uint64_t LastRecordOffset();

 private:
  // Skips all blocks that are completely before `initial_offset_`.
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  RecordType ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // `buffer_` must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  // Skips `bytes` bytes in the underlying log file.
  Status Skip(uint64_t bytes);

  // Reads the next block from the underlying log file.
  Status ReadNextBlock();

  int fd_;
  Status creation_status_;

  Reporter* const reporter_;
  bool const checksum_;
  const std::unique_ptr<uint8_t[]> backing_store_;
  Slice buffer_;
  bool eof_;  // Last `Read()` indicated EOF by returning < `kBlockSize`

  // Offset of the last record returned by `ReadRecord()`.
  uint64_t last_record_offset_;
  // Offset of the first location past the end of `buffer_`.
  uint64_t end_of_buffer_offset_;

  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (`initial_offset_ > 0`). In
  // particular, a run of `kMiddleType` and `kLastType` records can be silently
  // skipped in this mode
  bool resyncing_;
};

}  // namespace wal
}  // namespace tl
