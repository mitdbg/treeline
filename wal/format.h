// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <cstdlib>

// TreeLine's write-ahead log (WAL) on-disk format is identical to LevelDB's WAL
// on-disk format. TreeLine uses a slightly modified version of LevelDB's log
// writer/reader code to write to and read from the log.
//
// The log format stores entries as arbitrarily-sized byte strings (i.e., the
// log has no notion of keys and values; that is something that must be handled
// by the log user). Entries are packed into fixed size blocks (32 KiB).
// Typically one entry corresponds to one "record" in the log format. Each
// record's type and data are protected by a CRC32C checksum.
//
// Any entry that does not fit in one block is split into multiple records
// across as many blocks as needed. The record type (FULL, FIRST, MIDDLE, LAST)
// is used to indicate whether (i) the record's data corresponds to a FULL
// entry, or (ii) if it contains the FIRST part of the entry, (iii) a MIDDLE
// portion of the entry, or (iv) the LAST part of the entry.
//
//   log := block*
//   block := record* trailer?
//   record :=
//     checksum: uint32     // crc32c of type and data[] ; little-endian
//     length: uint16       // little-endian
//     type: uint8          // One of FULL, FIRST, MIDDLE, LAST
//     data: uint8[length]
//
// The main motivation for packing entries into fixed size blocks is to
// facilitate recovery when a record is corrupted in the middle of the log. If a
// record becomes corrupted, it becomes unsafe to trust its length field, making
// it difficult to know where the next record starts. With fixed-sized blocks,
// we can skip to the next block boundary to continue reading records (to
// hopefully minimize the amount of lost data).

namespace tl {
namespace wal {

enum class RecordType : uint8_t {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,

  // The enum values above should not be changed because they are part of the
  // log's on-disk format.

  // The enum values below are reserved for use by `wal::Reader` when the log is
  // replayed.

  // Indicates that the end of the log was reached.
  kEof = 5,

  // Returned whenever we find an invalid physical record.
  // Currently there are three situations in which this happens:
  // * The record has an invalid CRC (`wal::Reader::ReadPhysicalRecord()`
  //   reports a drop)
  // * The record is a 0-length record (No drop is reported)
  // * The record is below `wal::Reader`'s `initial_offset` (No drop is
  //   reported)
  kBadRecord = 6
};
inline constexpr uint8_t kMaxRecordType =
    static_cast<uint8_t>(RecordType::kBadRecord);

// Pack log records into 32 KiB blocks.
inline constexpr size_t kBlockSize = 32 * 1024;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
inline constexpr size_t kHeaderSize = 4 + 2 + 1;

}  // namespace wal
}  // namespace tl
