// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "writer.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

#include "util/coding.h"
#include "util/crc32c.h"
#include "wal/format.h"

namespace {

void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= tl::wal::kMaxRecordType; i++) {
    uint8_t t = static_cast<uint8_t>(i);
    type_crc[i] = tl::crc32c::Value(&t, 1);
  }
}

}  // namespace

namespace tl {
namespace wal {

Writer::Writer(const std::filesystem::path& log_path)
    : fd_(-1), block_offset_(0) {
  fd_ = open(log_path.c_str(), O_CREAT | O_WRONLY | O_APPEND,
             S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd_ < 0) {
    creation_status_ = Status::FromPosixError(log_path.string(), errno);
  }
  InitTypeCrc(type_crc_);
}

Status Writer::GetCreationStatus() const { return creation_status_; }

Writer::~Writer() {
  if (fd_ < 0) {
    return;
  }
  close(fd_);
}

Status Writer::AddEntry(const WriteOptions& options, const Slice& payload) {
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  size_t left = payload.size();

  // Fragment the record if necessary and emit it. Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7);
        AppendToLog(
            reinterpret_cast<const uint8_t*>("\x00\x00\x00\x00\x00\x00"),
            leftover);
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = RecordType::kFullType;
    } else if (begin) {
      type = RecordType::kFirstType;
    } else if (end) {
      type = RecordType::kLastType;
    } else {
      type = RecordType::kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, options.sync);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const uint8_t* ptr,
                                  size_t length, bool sync) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header.
  uint8_t buf[kHeaderSize];
  buf[4] = static_cast<uint8_t>(length & 0xff);
  buf[5] = static_cast<uint8_t>(length >> 8);
  buf[6] = static_cast<uint8_t>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc =
      crc32c::Extend(type_crc_[static_cast<uint8_t>(t)], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage

  // Store the checksum in the first 4 bytes of `buf`.
  EncodeFixed32(reinterpret_cast<char*>(buf), crc);

  // Write the header and the payload.
  Status s = AppendToLog(buf, kHeaderSize);
  if (s.ok()) {
    s = AppendToLog(ptr, length);
    if (s.ok() && sync) {
      s = SyncLog();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

Status Writer::AppendToLog(const uint8_t* data, size_t length) {
  ssize_t bytes_written = 0;
  while (length > 0) {
    bytes_written = write(fd_, data, length);
    if (bytes_written < 0) {
      return Status::FromPosixError("wal::Writer::AppendToLog()", errno);
    }
    length -= bytes_written;
    data += bytes_written;
  }
  return Status::OK();
}

Status Writer::SyncLog() {
  if (fdatasync(fd_) < 0) {
    return Status::FromPosixError("wal::Writer::SyncLog()", errno);
  }
  return Status::OK();
}

}  // namespace wal
}  // namespace tl
