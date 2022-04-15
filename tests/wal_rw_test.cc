// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "gtest/gtest.h"
#include "treeline/options.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "wal/reader.h"
#include "wal/writer.h"

namespace {

namespace fs = std::filesystem;
using namespace tl;
using namespace tl::wal;

const std::string kTestDir =
    "/tmp/tl-test-" + std::to_string(std::time(nullptr));
const std::string kTestLogFile = kTestDir + "/test.wal";

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) { return std::to_string(n); }

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class WALTest : public testing::Test {
 public:
  WALTest() : reading_(false), writer_(), reader_(), log_fd_(-1) {}

  void SetUp() override {
    fs::create_directory(kTestDir);
    writer_.reset(new Writer(kTestLogFile));
    reader_.reset(new Reader(kTestLogFile, &report_, /*checksum=*/true,
                             /*initial_offset=*/0));
    ASSERT_TRUE(writer_->GetCreationStatus().ok());
    ASSERT_TRUE(reader_->GetCreationStatus().ok());
    ASSERT_TRUE(fs::exists(kTestLogFile));
    log_fd_ = open(kTestLogFile.c_str(), O_RDWR);
    ASSERT_TRUE(log_fd_ > 0);  // fd 0 represents stdin
  }

  void TearDown() override {
    // Manually delete the reader and writer so that we can remove the backing
    // file too.
    writer_.reset();
    reader_.reset();
    close(log_fd_);

    // May throw an exception, so this cannot go in the destructor.
    fs::remove_all(kTestDir);
  }

  void ReopenForAppend() { writer_.reset(new Writer(kTestLogFile)); }

  void Write(const std::string& msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    const WriteOptions options;
    writer_->AddEntry(options, Slice(msg));
  }

  size_t WrittenBytes() const {
    struct stat statbuf;
    if (fstat(log_fd_, &statbuf) != 0) {
      return 0;
    }
    return statbuf.st_size;
  }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadEntry(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    uint8_t val = 0;
    ASSERT_TRUE(pread(log_fd_, &val, 1, offset) == 1);
    val += delta;
    ASSERT_TRUE(pwrite(log_fd_, &val, 1, offset) == 1);
  }

  void SetByte(int offset, uint8_t new_byte) {
    ASSERT_TRUE(pwrite(log_fd_, &new_byte, 1, offset) == 1);
  }

  void ShrinkSize(int bytes) {
    const size_t current_size = WrittenBytes();
    ASSERT_TRUE(current_size >= bytes);
    ASSERT_TRUE(ftruncate(log_fd_, current_size - bytes) == 0);
  }

  void FixChecksum(int header_offset, int len) {
    // NOTE: len is the data length.
    const size_t record_size = kHeaderSize + len;
    uint8_t record[record_size];

    // Read in the data first.
    ASSERT_TRUE(pread(log_fd_, record, record_size, header_offset) ==
                record_size);

    // Compute crc of type/data.
    uint32_t crc = tl::crc32c::Value(&record[6], 1 + len);
    crc = tl::crc32c::Mask(crc);

    // The checksum goes in the first 4 bytes of the record.
    EncodeFixed32(reinterpret_cast<char*>(record), crc);

    // Write the record back.
    ASSERT_TRUE(pwrite(log_fd_, record, record_size, header_offset) ==
                record_size);
  }

  void ForceReadError() {
    // Empty the log file to "force" a read error.
    ASSERT_TRUE(ftruncate(log_fd_, 0) == 0);
  }

  size_t DroppedBytes() const { return report_.dropped_bytes_; }

  std::string ReportMessage() const { return report_.message_; }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

  void StartReadingAt(uint64_t initial_offset) {
    reader_.reset(
        new Reader(kTestLogFile, &report_, /*checksum=*/true, initial_offset));
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    Reader offset_reader(kTestLogFile, &report_, /*checksum=*/true,
                         WrittenBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader.ReadEntry(&record, &scratch));
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    Reader offset_reader(kTestLogFile, &report_, /*checksum=*/true,
                         initial_offset);

    // Read all records from expected_record_offset through the last one.
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_;
         ++expected_record_offset) {
      Slice record;
      std::string scratch;
      ASSERT_TRUE(offset_reader.ReadEntry(&record, &scratch));
      ASSERT_EQ(initial_offset_record_sizes_[expected_record_offset],
                record.size());
      ASSERT_EQ(initial_offset_last_record_offsets_[expected_record_offset],
                offset_reader.LastRecordOffset());
      ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
    }
  }

 private:
  class ReportCollector : public Reader::Reporter {
   public:
    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }

    size_t dropped_bytes_;
    std::string message_;
  };

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

  ReportCollector report_;
  bool reading_;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<Reader> reader_;
  int log_fd_;
};

size_t WALTest::initial_offset_record_sizes_[] = {
    10000,  // Two sizable records in first block
    10000,
    2 * kBlockSize - 1000,  // Span three blocks
    1,
    13716,                     // Consume all but two bytes of block 3.
    kBlockSize - kHeaderSize,  // Consume the entirety of block 4.
};

uint64_t WALTest::initial_offset_last_record_offsets_[] = {
    0,
    kHeaderSize + 10000,
    2 * (kHeaderSize + 10000),
    2 * (kHeaderSize + 10000) + (2 * kBlockSize - 1000) + 3 * kHeaderSize,
    2 * (kHeaderSize + 10000) + (2 * kBlockSize - 1000) + 3 * kHeaderSize +
        kHeaderSize + 1,
    3 * kBlockSize,
};

// WALTest::initial_offset_last_record_offsets_ must be defined before this.
int WALTest::num_initial_offset_records_ =
    sizeof(WALTest::initial_offset_last_record_offsets_) / sizeof(uint64_t);

TEST_F(WALTest, Empty) { ASSERT_EQ("EOF", Read()); }

TEST_F(WALTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_F(WALTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(WALTest, ShortTrailer) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, AlignedEof) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, OpenForAppend) {
  Write("hello");
  ReopenForAppend();
  Write("world");
  ASSERT_EQ("hello", Read());
  ASSERT_EQ("world", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in wal/reader.cc follow:

TEST_F(WALTest, ReadError) {
  Write("foo");
  ForceReadError();
  ASSERT_EQ("EOF", Read());
  // We empty the file when calling `ForceReadError()`.
  ASSERT_EQ(0, DroppedBytes());
  // No error message is expected because the log is empty.
  ASSERT_EQ("", ReportMessage());
}

TEST_F(WALTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  // LevelDB expects 3 dropped bytes, but TreeLine expects 10. This is because
  // TreeLine immediately drops the record if it does not recognize the record
  // type.
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_F(WALTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error.
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(WALTest, BadLength) {
  const int kPayloadSize = kBlockSize - kHeaderSize;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4].
  IncrementByte(4, 1);
  ASSERT_EQ("foo", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("bad record length"));
}

TEST_F(WALTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(WALTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST_F(WALTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, static_cast<uint8_t>(RecordType::kMiddleType));
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(WALTest, UnexpectedLastType) {
  Write("foo");
  SetByte(6, static_cast<uint8_t>(RecordType::kLastType));
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(WALTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  SetByte(6, static_cast<uint8_t>(RecordType::kFirstType));
  FixChecksum(0, 3);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(WALTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, static_cast<uint8_t>(RecordType::kFirstType));
  FixChecksum(0, 3);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(WALTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(WALTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(WALTest, SkipIntoMultiRecord) {
  // Consider a fragmented record:
  //    first(R1), middle(R1), last(R1), first(R2)
  // If initial_offset points to a record after first(R1) but before first(R2)
  // incomplete fragment errors are not actual errors, and must be suppressed
  // until a new first or full record is encountered.
  Write(BigString("foo", 3 * kBlockSize));
  Write("correct");
  StartReadingAt(kBlockSize);

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("EOF", Read());
}

TEST_F(WALTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("EOF", Read());
  const size_t dropped = DroppedBytes();
  ASSERT_LE(dropped, 2 * kBlockSize + 100);
  ASSERT_GE(dropped, 2 * kBlockSize);
}

TEST_F(WALTest, ReadStart) { CheckInitialOffsetRecord(0, 0); }

TEST_F(WALTest, ReadSecondOneOff) { CheckInitialOffsetRecord(1, 1); }

TEST_F(WALTest, ReadSecondTenThousand) { CheckInitialOffsetRecord(10000, 1); }

TEST_F(WALTest, ReadSecondStart) { CheckInitialOffsetRecord(10007, 1); }

TEST_F(WALTest, ReadThirdOneOff) { CheckInitialOffsetRecord(10008, 2); }

TEST_F(WALTest, ReadThirdStart) { CheckInitialOffsetRecord(20014, 2); }

TEST_F(WALTest, ReadFourthOneOff) { CheckInitialOffsetRecord(20015, 3); }

TEST_F(WALTest, ReadFourthFirstBlockTrailer) {
  CheckInitialOffsetRecord(kBlockSize - 4, 3);
}

TEST_F(WALTest, ReadFourthMiddleBlock) {
  CheckInitialOffsetRecord(kBlockSize + 1, 3);
}

TEST_F(WALTest, ReadFourthLastBlock) {
  CheckInitialOffsetRecord(2 * kBlockSize + 1, 3);
}

TEST_F(WALTest, ReadFourthStart) {
  CheckInitialOffsetRecord(
      2 * (kHeaderSize + 1000) + (2 * kBlockSize - 1000) + 3 * kHeaderSize, 3);
}

TEST_F(WALTest, ReadInitialOffsetIntoBlockPadding) {
  CheckInitialOffsetRecord(3 * kBlockSize - 3, 5);
}

TEST_F(WALTest, ReadEnd) { CheckOffsetPastEndReturnsNoRecords(0); }

TEST_F(WALTest, ReadPastEnd) { CheckOffsetPastEndReturnsNoRecords(5); }

}  // namespace
