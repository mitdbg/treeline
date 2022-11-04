#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "gflags/gflags.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      exit(1);                                                               \
    }                                                                        \
  } while (0)

DEFINE_uint32(seed, 42, "PRNG seed for reproducibility.");
DEFINE_uint32(trials, 1, "Number of repetitions.");
DEFINE_string(write_path, "", "Path where the read data is stored.");
DEFINE_uint32(size_mib, 10240, "Size of the file to read.");
DEFINE_uint32(iterations, 100000, "Number of page reads to make.");
DEFINE_uint32(warmup, 1000, "Number of warmup page reads to make.");
DEFINE_string(mode, "all", "Set to one of {all, power_two, single}.");
DEFINE_string(out_path, ".", "Where to write the output files.");
DEFINE_bool(interleave_alloc, false,
            "Set to true to allocate the files in an interleaved fashion.");

namespace {

constexpr size_t kPageSize = 4096;
constexpr size_t kAlignment = 4096;
constexpr size_t kBufferSize = kPageSize * 16;
constexpr size_t kNumFiles = 5;

const std::vector<size_t> kAllPages = ([]() {
  std::vector<size_t> pages;
  pages.resize(16);
  std::iota(pages.begin(), pages.end(), 1);
  return pages;
})();
const std::vector<size_t> kPowerTwoPages = {1, 2, 4, 8, 16};
const std::vector<size_t> kSingle = {1};

namespace fs = std::filesystem;

class SegmentFile {
 public:
  SegmentFile() : fd_(-1), pages_in_segment_(0), name_() {}

  SegmentFile(fs::path name, uint32_t pages_in_segment)
      : fd_(-1), pages_in_segment_(pages_in_segment), name_(std::move(name)) {
    CHECK_ERROR(fd_ = open(name_.c_str(), O_CREAT | O_RDWR | O_SYNC | O_DIRECT,
                           S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));
  }

  ~SegmentFile() {
    if (fd_ < 0) return;
    close(fd_);
  }

  SegmentFile(const SegmentFile&) = delete;
  const SegmentFile& operator=(const SegmentFile&) = delete;

  SegmentFile(SegmentFile&& other)
      : fd_(other.fd_),
        pages_in_segment_(other.pages_in_segment_),
        name_(other.name_) {
    other.fd_ = -1;
  }

  SegmentFile& operator=(SegmentFile&& other) {
    fd_ = other.fd_;
    pages_in_segment_ = other.pages_in_segment_;
    name_ = other.name_;
    other.fd_ = -1;
    return *this;
  }

  size_t size() const {
    struct stat file_status;
    CHECK_ERROR(fstat(fd_, &file_status));
    assert(file_status.st_size >= 0);
    return file_status.st_size;
  }

  const fs::path& path() const { return name_; }
  int fd() const { return fd_; }
  uint32_t pages_in_segment() const { return pages_in_segment_; }

 private:
  int fd_;
  uint32_t pages_in_segment_;
  fs::path name_;
};

void FillBuffer(void* buffer) {
  std::mt19937 prng(FLAGS_seed);
  const size_t iterations = kBufferSize / sizeof(uint32_t);
  uint32_t* buffer_as_int = reinterpret_cast<uint32_t*>(buffer);
  for (size_t i = 0; i < iterations; ++i) {
    buffer_as_int[i] = prng();
  }
}

size_t BytesPerFile() { return FLAGS_size_mib * 1024ULL * 1024ULL / kNumFiles; }

size_t PagesPerFile() { return BytesPerFile() / kPageSize; }

std::vector<SegmentFile> OpenAndInitializeFiles(void* buffer) {
  fs::path write_path(FLAGS_write_path);
  fs::create_directories(write_path);

  std::vector<SegmentFile> files;
  for (uint32_t i = 0; i < kNumFiles; ++i) {
    files.emplace_back(write_path / ("sf-" + std::to_string(i)), 1U << i);
  }

  const size_t bytes_per_file = BytesPerFile();

  FillBuffer(buffer);
  if (FLAGS_interleave_alloc) {
    if (std::all_of(files.begin(), files.end(),
                    [bytes_per_file](const SegmentFile& sf) {
                      return sf.size() >= bytes_per_file;
                    })) {
      std::cerr << "> Files are already large enough. Skipping initialization."
                << std::endl;
    } else {
      std::cerr << "> Performing interleaved initialization." << std::endl;
      std::mt19937 prng(FLAGS_seed + 2);
      size_t total_segments = 0;
      std::vector<size_t> file_segment_counts;
      for (const auto& sf : files) {
        const size_t segments_in_file = PagesPerFile() / sf.pages_in_segment();
        CHECK_ERROR(lseek(sf.fd(), 0, SEEK_SET));
        file_segment_counts.push_back(segments_in_file);
        total_segments += segments_in_file;
      }
      while (total_segments > 0) {
        std::discrete_distribution<size_t> file_dist(
            file_segment_counts.begin(), file_segment_counts.end());
        const size_t next_file = file_dist(prng);
        assert(file_segment_counts[next_file] > 0);

        const SegmentFile& sf = files[next_file];
        CHECK_ERROR(write(sf.fd(), buffer, sf.pages_in_segment() * kPageSize));
        --file_segment_counts[next_file];
        --total_segments;
      }
    }
  } else {
    // Simple sequential initialization.
    for (const auto& sf : files) {
      if (sf.size() >= bytes_per_file) {
        std::cerr << "> " << sf.path().filename()
                  << " is already large enough. Skipping initialization."
                  << std::endl;
        continue;
      }
      std::cerr << "> Initializing " << sf.path().filename() << std::endl;

      const uint32_t pages_in_segment = sf.pages_in_segment();
      const size_t num_writes = PagesPerFile() / pages_in_segment;
      CHECK_ERROR(lseek(sf.fd(), 0, SEEK_SET));
      for (size_t i = 0; i < num_writes; ++i) {
        CHECK_ERROR(write(sf.fd(), buffer, pages_in_segment * kPageSize));
      }
    }
  }

  return files;
}

auto RunSingleBenchmark(void* buffer, const std::vector<SegmentFile>& files) {
  const size_t num_pages_per_file = PagesPerFile();
  std::uniform_int_distribution<uint32_t> file_dist(0, files.size() - 1);
  std::uniform_int_distribution<size_t> page_offset_dist(
      0, num_pages_per_file - 1);
  std::mt19937 prng(FLAGS_seed + 1);
  // - Select a file
  // - Select which page to read
  auto run_workload = [&prng, &files, &file_dist, &page_offset_dist,
                       buffer](const uint32_t pages_to_read) {
    int64_t pages_left = pages_to_read;
    while (pages_left > 0) {
      const SegmentFile& file = files[file_dist(prng)];
      const size_t byte_offset = page_offset_dist(prng) * kPageSize;
      CHECK_ERROR(pread(file.fd(), buffer, kPageSize, byte_offset));
      pages_left -= 1;
    }
    // Number of pages actually read.
    return -pages_left + pages_to_read;
  };

  std::cerr << "> Running 'single' warmup: " << FLAGS_warmup << " pages..."
            << std::endl;
  run_workload(FLAGS_warmup);

  std::cerr << "> Done warmup. Running 'single' workload: " << FLAGS_iterations
            << " pages..." << std::endl;
  const auto start = std::chrono::steady_clock::now();
  const int64_t pages_read = run_workload(FLAGS_iterations);
  const auto end = std::chrono::steady_clock::now();
  std::cerr << "Done workload." << std::endl;

  return std::make_pair(pages_read, end - start);
}

auto RunPowerTwoBenchmark(void* buffer, const std::vector<SegmentFile>& files) {
  using Dist = std::uniform_int_distribution<size_t>;
  const size_t num_pages_per_file = PagesPerFile();
  Dist file_dist(0, files.size() - 1);
  std::vector<Dist> seg_offset_dists;
  for (const auto& file : files) {
    const size_t segs_in_file = num_pages_per_file / file.pages_in_segment();
    seg_offset_dists.emplace_back(0, segs_in_file - 1);
  }

  std::mt19937 prng(FLAGS_seed + 1);
  // - Select a file
  // - Select which segment to read
  auto run_workload = [&prng, &files, buffer, &file_dist,
                       &seg_offset_dists](const uint32_t pages_to_read) {
    int64_t pages_left = pages_to_read;
    while (pages_left > 0) {
      const size_t file_idx = file_dist(prng);
      const SegmentFile& file = files[file_idx];
      const size_t num_pages = file.pages_in_segment();
      const size_t byte_offset =
          seg_offset_dists[file_idx](prng) * kPageSize * num_pages;
      CHECK_ERROR(pread(file.fd(), buffer, num_pages * kPageSize, byte_offset));
      pages_left -= num_pages;
    }
    // Returns the number of pages actually read.
    return -pages_left + pages_to_read;
  };

  std::cerr << "> Running 'power_two' warmup: " << FLAGS_warmup << " pages..."
            << std::endl;
  run_workload(FLAGS_warmup);

  std::cerr << "> Done warmup. Running 'power_two' workload: "
            << FLAGS_iterations << " pages..." << std::endl;
  const auto start = std::chrono::steady_clock::now();
  const int64_t pages_read = run_workload(FLAGS_iterations);
  const auto end = std::chrono::steady_clock::now();
  std::cerr << "Done workload." << std::endl;

  return std::make_pair(pages_read, end - start);
}

auto RunAllBenchmark(void* buffer, const std::vector<SegmentFile>& files) {
  using Dist = std::uniform_int_distribution<size_t>;
  const size_t num_pages_per_file = PagesPerFile();

  Dist start_end_dist(0, 1);
  Dist file_dist(0, files.size() - 1);
  std::vector<Dist> seg_offset_dists;  // Select which offset to read.
  for (const auto& file : files) {
    const size_t segs_in_file = num_pages_per_file / file.pages_in_segment();
    seg_offset_dists.emplace_back(0, segs_in_file - 1);
  }
  std::vector<Dist> seg_pages_dists;  // Select the number of pages to read.
  for (const auto& file : files) {
    seg_pages_dists.emplace_back(1, file.pages_in_segment());
  }

  std::mt19937 prng(FLAGS_seed + 1);
  // - Select a file
  // - Select which segment to read
  // - Select the number of pages from the segment to read
  // - Select whether to read from the start or end of the segment
  auto run_workload = [&prng, &files, buffer, &start_end_dist, &file_dist,
                       &seg_offset_dists,
                       &seg_pages_dists](const uint32_t pages_to_read) {
    int64_t pages_left = pages_to_read;
    while (pages_left > 0) {
      const size_t file_idx = file_dist(prng);
      const SegmentFile& file = files[file_idx];
      const size_t seg_offset = seg_offset_dists[file_idx](prng);
      const size_t num_pages = seg_pages_dists[file_idx](prng);
      const size_t start_end = start_end_dist(prng);
      const size_t byte_offset =
          start_end == 0
              ? (seg_offset * kPageSize * file.pages_in_segment())
              : ((seg_offset + 1) * kPageSize * file.pages_in_segment() -
                 num_pages * kPageSize);
      CHECK_ERROR(pread(file.fd(), buffer, num_pages * kPageSize, byte_offset));
      pages_left -= num_pages;
    }
    // Returns the number of pages actually read.
    return -pages_left + pages_to_read;
  };

  std::cerr << "> Running 'all' warmup: " << FLAGS_warmup << " pages..."
            << std::endl;
  run_workload(FLAGS_warmup);

  std::cerr << "> Done warmup. Running 'all' workload: " << FLAGS_iterations
            << " pages..." << std::endl;
  const auto start = std::chrono::steady_clock::now();
  const int64_t pages_read = run_workload(FLAGS_iterations);
  const auto end = std::chrono::steady_clock::now();
  std::cerr << "Done workload." << std::endl;

  return std::make_pair(pages_read, end - start);
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run a randomized read microbenchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_write_path.empty() || FLAGS_out_path.empty()) {
    std::cerr << "ERROR: --write_path and --out_path both must be set."
              << std::endl;
    return 1;
  }
  if (FLAGS_size_mib % kNumFiles != 0) {
    std::cerr << "ERROR: File size needs to be divisible by " << kNumFiles
              << std::endl;
    return 1;
  }

  void* const buffer = aligned_alloc(kAlignment, kBufferSize);
  assert(buffer != nullptr);

  const std::vector<SegmentFile> files = OpenAndInitializeFiles(buffer);

  const auto out_dir = fs::path(FLAGS_out_path);
  std::ofstream out(out_dir / "results.csv");

  out << "pages_read,elapsed_time_ns" << std::endl;
  for (uint32_t i = 0; i < FLAGS_trials; ++i) {
    if (FLAGS_mode == "single") {
      const auto [pages_read, time_ns] = RunSingleBenchmark(buffer, files);
      out << pages_read << "," << time_ns.count() << std::endl;
    } else if (FLAGS_mode == "all") {
      const auto [pages_read, time_ns] = RunAllBenchmark(buffer, files);
      out << pages_read << "," << time_ns.count() << std::endl;
    } else if (FLAGS_mode == "power_two") {
      const auto [pages_read, time_ns] = RunPowerTwoBenchmark(buffer, files);
      out << pages_read << "," << time_ns.count() << std::endl;
    } else {
      throw std::runtime_error("Unrecognized mode.");
    }
  }

  free(buffer);
  return 0;
}
