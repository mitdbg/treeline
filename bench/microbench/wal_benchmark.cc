// Benchmarks that measure the write performance of the write-ahead log.
//
// Build the benchmarks by enabling the `TL_BUILD_BENCHMARKS` option when
// configuring the project. Then run the `wal` executable under `bench`. You
// need to specify a file path for the log using `--log_path`.
//
//   mkdir build && cd build
//   cmake -DCMAKE_BUILD_TYPE=Release -DTL_BUILD_BENCHMARKS=ON ..
//   make -j
//   ./bench/wal --log_path=<path where the log file should be written>
//
// clang-format off
//
// Example run (log is written to a 1 TB Intel NVMe SSD (SSDPE2KX010T8)):
//
// 2021-02-17T21:39:07-05:00
// Running ./bench/wal --log_path=/flash1/geoffxy/test.wal
// --benchmark_filter="Fdatasync_KiB" Run on (40 X 3900 MHz CPU s) CPU Caches:
//   L1 Data 32 KiB (x20)
//   L1 Instruction 32 KiB (x20)
//   L2 Unified 1024 KiB (x20)
//   L3 Unified 28160 KiB (x1)
// Load Average: 0.35, 0.51, 0.45
// ---------------------------------------------------------------------------------------
// Benchmark                             Time             CPU   Iterations UserCounters...
// ---------------------------------------------------------------------------------------
// Fdatasync_KiB/1/real_time          49.4 us         9.26 us        12130 bytes_per_second=19.7547M/s
// Fdatasync_KiB/2/real_time          53.5 us         11.1 us        13816 bytes_per_second=36.4972M/s
// Fdatasync_KiB/4/real_time          68.4 us         17.6 us        10158 bytes_per_second=57.1129M/s
// Fdatasync_KiB/8/real_time          72.0 us         16.9 us         9679 bytes_per_second=108.553M/s
// Fdatasync_KiB/16/real_time         78.7 us         21.0 us         8411 bytes_per_second=198.581M/s
// Fdatasync_KiB/32/real_time         90.9 us         26.0 us         7686 bytes_per_second=343.969M/s
// Fdatasync_KiB/64/real_time          125 us         40.5 us         5928 bytes_per_second=498.285M/s
// Fdatasync_KiB/128/real_time         181 us         67.9 us         3799 bytes_per_second=689.097M/s
// Fdatasync_KiB/256/real_time         289 us          115 us         2477 bytes_per_second=864.932M/s
// Fdatasync_KiB/512/real_time         509 us          216 us         1383 bytes_per_second=981.596M/s
// Fdatasync_KiB/1024/real_time       1011 us          413 us          710 bytes_per_second=989.121M/s
//
// clang-format on

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "bench/common/data.h"
#include "benchmark/benchmark.h"
#include "db/format.h"
#include "db/memtable.h"
#include "db/options.h"
#include "gflags/gflags.h"
#include "treeline/options.h"
#include "util/coding.h"
#include "wal/writer.h"

namespace {

using namespace tl;
namespace fs = std::filesystem;

bool ValidateLog(const char* flagname, const std::string& path) {
  if (!path.empty()) return true;
  std::cerr << "ERROR: --log_path cannot be empty." << std::endl;
  return false;
}

DEFINE_string(log_path, "", "Where the log should be written.");
DEFINE_validator(log_path, &ValidateLog);

std::string SerializeSingleRecord(const Slice& key, const Slice& value) {
  // Record serialization format:
  // - Write type (1 byte)
  // - Key length (varint32)
  // - Key bytes
  // - Value bytes
  // We do not need to encode the value size because the WAL already encodes the
  // total serialized record size.

  std::string serialized_record;
  // In the worst case the key length takes up 4 bytes.
  serialized_record.reserve(1 + 4 + key.size() + value.size());
  serialized_record.push_back(static_cast<uint8_t>(format::WriteType::kWrite));
  PutVarint32(&serialized_record, key.size());
  serialized_record.append(key.data(), key.size());
  serialized_record.append(value.data(), value.size());
  return serialized_record;
}

class WriteBatch {
 public:
  WriteBatch() : size_(0) { Clear(); }

  void Put(const Slice& key, const Slice& value) {
    buffer_.push_back(static_cast<uint8_t>(format::WriteType::kWrite));
    PutLengthPrefixedSlice(&buffer_, key);
    PutLengthPrefixedSlice(&buffer_, value);
    ++size_;
  }

  Slice Serialize() const {
    EncodeFixed32(buffer_.data(), size_);
    return Slice(buffer_.data(), buffer_.size());
  }

  uint32_t Size() const { return size_; }

  void Clear() {
    size_ = 0;
    buffer_.clear();
    // Reserves the first 4 bytes for the size
    PutFixed32(&buffer_, size_);
  }

  void AddToMemTable(MemTable& mtable) const {
    Slice source(buffer_);
    source.remove_prefix(4);  // Encoded size

    Status s;
    for (uint32_t i = 0; i < size_; ++i) {
      Slice key, value;
      bool key_success = GetLengthPrefixedSlice(&source, &key);
      bool value_success = GetLengthPrefixedSlice(&source, &value);
      if (!key_success || !value_success) {
        throw std::runtime_error("Failed to read record from batch.");
      }
      s = mtable.Put(key, value);
      if (!s.ok()) {
        throw std::runtime_error("Failed to add to memtable.");
      }
    }
  }

 private:
  // Batch Format:
  // - Batch size (4 byte unsigned integer)
  // - Records
  // Record Format:
  // - Write type (1 byte)
  // - Length prefixed key (varint32 length followed by data)
  // - Length prefixed value (varint32 length followed by data)
  mutable std::string buffer_;
  uint32_t size_;
};

void LogWriteSingle_64MiB(benchmark::State& state, bool sync) {
  constexpr size_t kDatasetSizeMiB = 64;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  fs::remove(FLAGS_log_path);
  wal::Writer writer(FLAGS_log_path);
  if (!writer.GetCreationStatus().ok()) {
    throw std::runtime_error("Failed to create log!");
  }

  Status s;
  WriteOptions write_options;
  write_options.sync = sync;
  for (auto _ : state) {
    for (const auto& record : dataset) {
      s = writer.AddEntry(write_options,
                          SerializeSingleRecord(record.key(), record.value()));
      if (!s.ok()) {
        throw std::runtime_error("Failed to log record!");
      }
    }
  }
  state.counters["ops_per_second"] = benchmark::Counter(
      state.iterations() * dataset.size(), benchmark::Counter::kIsRate);
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);

  fs::remove(FLAGS_log_path);
}

void LogWriteBatch_64MiB(benchmark::State& state, bool sync) {
  constexpr size_t kDatasetSizeMiB = 64;
  const uint32_t batch_size = state.range(0);
  const size_t record_size = state.range(1);

  bench::U64Dataset::GenerateOptions options;
  options.record_size = record_size;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  fs::remove(FLAGS_log_path);
  wal::Writer writer(FLAGS_log_path);
  if (!writer.GetCreationStatus().ok()) {
    throw std::runtime_error("Failed to create log!");
  }

  Status s;
  WriteOptions write_options;
  write_options.sync = sync;
  for (auto _ : state) {
    WriteBatch batch;
    for (const auto& record : dataset) {
      batch.Put(record.key(), record.value());
      if (batch.Size() >= batch_size) {
        s = writer.AddEntry(write_options, batch.Serialize());
        if (!s.ok()) {
          throw std::runtime_error("Failed to log record!");
        }
        batch.Clear();
      }
    }

    if (batch.Size() > 0) {
      s = writer.AddEntry(write_options, batch.Serialize());
      if (!s.ok()) {
        throw std::runtime_error("Failed to log record!");
      }
      batch.Clear();
    }
  }
  state.counters["ops_per_second"] = benchmark::Counter(
      state.iterations() * dataset.size(), benchmark::Counter::kIsRate);
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);

  fs::remove(FLAGS_log_path);
}

void MemTableSingle_64MiB(benchmark::State& state, bool use_log, bool sync) {
  constexpr size_t kDatasetSizeMiB = 64;
  bench::U64Dataset::GenerateOptions options;
  options.record_size = state.range(0);
  options.shuffle = true;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  fs::remove(FLAGS_log_path);
  wal::Writer writer(FLAGS_log_path);
  if (!writer.GetCreationStatus().ok()) {
    throw std::runtime_error("Failed to create log!");
  }

  Status s;
  WriteOptions write_options;
  write_options.sync = sync;
  for (auto _ : state) {
    MemTable mtable(tl::MemTableOptions{});
    for (const auto& record : dataset) {
      if (use_log) {
        s = writer.AddEntry(
            write_options, SerializeSingleRecord(record.key(), record.value()));
        if (!s.ok()) {
          throw std::runtime_error("Failed to log record!");
        }
      }
      s = mtable.Put(record.key(), record.value());
      if (!s.ok()) {
        throw std::runtime_error("Failed to put record into the memtable!");
      }
    }
  }
  state.counters["ops_per_second"] = benchmark::Counter(
      state.iterations() * dataset.size(), benchmark::Counter::kIsRate);
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);

  fs::remove(FLAGS_log_path);
}

void MemTableBatch_64MiB(benchmark::State& state, bool use_log, bool sync) {
  constexpr size_t kDatasetSizeMiB = 64;
  const uint32_t batch_size = state.range(0);
  const size_t record_size = state.range(1);

  bench::U64Dataset::GenerateOptions options;
  options.record_size = record_size;
  options.shuffle = true;
  bench::U64Dataset dataset =
      bench::U64Dataset::Generate(kDatasetSizeMiB, options);

  fs::remove(FLAGS_log_path);
  wal::Writer writer(FLAGS_log_path);
  if (!writer.GetCreationStatus().ok()) {
    throw std::runtime_error("Failed to create log!");
  }

  Status s;
  WriteOptions write_options;
  write_options.sync = sync;
  for (auto _ : state) {
    MemTable mtable(tl::MemTableOptions{});
    WriteBatch batch;
    for (const auto& record : dataset) {
      batch.Put(record.key(), record.value());
      if (batch.Size() >= batch_size) {
        if (use_log) {
          s = writer.AddEntry(write_options, batch.Serialize());
          if (!s.ok()) {
            throw std::runtime_error("Failed to log record!");
          }
        }
        batch.AddToMemTable(mtable);
        batch.Clear();
      }
    }

    if (batch.Size() > 0) {
      if (use_log) {
        s = writer.AddEntry(write_options, batch.Serialize());
        if (!s.ok()) {
          throw std::runtime_error("Failed to log record!");
        }
      }
      batch.AddToMemTable(mtable);
      batch.Clear();
    }
  }
  state.counters["ops_per_second"] = benchmark::Counter(
      state.iterations() * dataset.size(), benchmark::Counter::kIsRate);
  state.SetBytesProcessed(state.iterations() * kDatasetSizeMiB * 1024 * 1024);

  fs::remove(FLAGS_log_path);
}

void Fdatasync_KiB(benchmark::State& state) {
  const size_t write_size = state.range(0) * 1024;
  std::unique_ptr<char[]> buf(new char[write_size]);

  fs::remove(FLAGS_log_path);
  int fd = open(FLAGS_log_path.c_str(), O_TRUNC | O_CREAT | O_WRONLY,
                S_IRUSR | S_IWUSR);
  if (fd < 0) {
    throw std::runtime_error("Failed to create file for writing.");
  }

  for (auto _ : state) {
    state.PauseTiming();
    if (write(fd, buf.get(), write_size) != write_size) {
      close(fd);
      throw std::runtime_error("Failed to write buffer.");
    }
    state.ResumeTiming();

    if (fdatasync(fd) != 0) {
      close(fd);
      throw std::runtime_error("Failed to call fdatasync().");
    }
  }
  close(fd);
  fs::remove(FLAGS_log_path);

  state.SetBytesProcessed(state.iterations() * write_size);
}

// Writing to the log using a batch of records
BENCHMARK_CAPTURE(LogWriteBatch_64MiB, no_sync, /*sync=*/false)
    ->ArgsProduct({{32, 64, 128, 256, 512, 1024, 2048, 4096},
                   {16, 512}})  // (Batch size, Record size in bytes)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(LogWriteBatch_64MiB, sync, /*sync=*/true)
    ->ArgsProduct({{32, 64, 128, 256, 512, 1024, 2048, 4096},
                   {16, 512}})  // (Batch size, Record size in bytes)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// Writing to the log using a single record at a time
BENCHMARK_CAPTURE(LogWriteSingle_64MiB, no_sync, /*sync=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(LogWriteSingle_64MiB, sync, /*sync=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// Writing to a memtable using a single record at a time, with and without a log
BENCHMARK_CAPTURE(MemTableSingle_64MiB, no_log, /*use_log=*/false,
                  /*sync=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(MemTableSingle_64MiB, log_no_sync, /*use_log=*/true,
                  /*sync=*/false)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(MemTableSingle_64MiB, log_sync, /*use_log=*/true,
                  /*sync=*/true)
    ->Arg(16)  // Record size in bytes
    ->Arg(512)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// Writing to a memtable using a batch of records, with and without a log
BENCHMARK_CAPTURE(MemTableBatch_64MiB, no_log, /*use_log=*/false,
                  /*sync=*/false)
    ->ArgsProduct({{32, 64, 128, 256, 512, 1024, 2048, 4096},
                   {16, 512}})  // (Batch size, Record size in bytes)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(MemTableBatch_64MiB, log_no_sync, /*use_log=*/true,
                  /*sync=*/false)
    ->ArgsProduct({{32, 64, 128, 256, 512, 1024, 2048, 4096},
                   {16, 512}})  // (Batch size, Record size in bytes)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_CAPTURE(MemTableBatch_64MiB, log_sync, /*use_log=*/true,
                  /*sync=*/true)
    ->ArgsProduct({{32, 64, 128, 256, 512, 1024, 2048, 4096},
                   {16, 512}})  // (Batch size, Record size in bytes)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// Calling fdatasync() on a file descriptor after writing a block of data
BENCHMARK(Fdatasync_KiB)
    ->RangeMultiplier(2)
    ->Range(1, 1024)  // KiB
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Measure the write-ahead log overhead.");
  benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
