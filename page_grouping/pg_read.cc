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
DEFINE_string(file, "", "Path to the file to read.");
DEFINE_uint32(size_mib, 10240, "Size of the file to read.");
DEFINE_uint32(iterations, 100000, "Number of page reads to make.");
DEFINE_uint32(warmup, 1000, "Number of warmup page reads to make.");
DEFINE_string(mode, "all", "Set to one of {all, power_two, single}.");
DEFINE_string(out_path, ".", "Where to write the output files.");
DEFINE_bool(rand_init, false, "If set, initialize the file in random order.");

namespace {

constexpr size_t kPageSize = 4096;
constexpr size_t kAlignment = 4096;
constexpr size_t kBufferSize = kPageSize * 16;

const std::vector<size_t> kAllPages = ([]() {
  std::vector<size_t> pages;
  pages.resize(16);
  std::iota(pages.begin(), pages.end(), 1);
  return pages;
})();
const std::vector<size_t> kPowerTwoPages = {1, 2, 4, 8, 16};
const std::vector<size_t> kSingle = {1};

namespace fs = std::filesystem;

void FillBuffer(void* buffer) {
  std::mt19937 prng(FLAGS_seed);
  const size_t iterations = kBufferSize / sizeof(uint32_t);
  uint32_t* buffer_as_int = reinterpret_cast<uint32_t*>(buffer);
  for (size_t i = 0; i < iterations; ++i) {
    buffer_as_int[i] = prng();
  }
}

// Opens the file and returns a file descriptor. Performs initialization if
// needed.
int OpenAndInitializeFile(void* buffer) {
  int fd = -1;
  CHECK_ERROR(fd =
                  open(FLAGS_file.c_str(), O_CREAT | O_RDWR | O_SYNC | O_DIRECT,
                       S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));

  const size_t file_size_bytes = FLAGS_size_mib * 1024ULL * 1024ULL;
  if (fs::file_size(FLAGS_file) < file_size_bytes) {
    std::cerr << "> Initializing the file (" << FLAGS_size_mib << " MiB)..."
              << std::endl;
    FillBuffer(buffer);
    if (FLAGS_rand_init) {
      const size_t num_writes = file_size_bytes / kPageSize;
      // Generate a "random" initialization order.
      std::vector<size_t> page_write_order;
      page_write_order.resize(num_writes);
      std::iota(page_write_order.begin(), page_write_order.end(), 0);
      std::mt19937 prng(FLAGS_seed + 2);
      std::shuffle(page_write_order.begin(), page_write_order.end(), prng);
      CHECK_ERROR(fallocate(fd, 0, 0, file_size_bytes));

      // `buffer` is filled with random data and is 16 pages long. We "randomly"
      // select a chunk of data in the buffer to write to the offset.
      //
      // N.B. This allocation pattern also does not exactly mimic page grouping
      // because all the pages in a logically contiguous segments are always
      // allocated together.
      std::uniform_int_distribution<uint32_t> buffer_slot(0, 15);
      uint8_t* buf = reinterpret_cast<uint8_t*>(buffer);
      for (const auto& page_offset : page_write_order) {
        const uint32_t buffer_slot_idx = buffer_slot(prng);
        CHECK_ERROR(pwrite(fd, buf + buffer_slot_idx * kPageSize, kPageSize,
                           page_offset * kPageSize));
      }

    } else {
      const size_t num_writes = file_size_bytes / kBufferSize;
      CHECK_ERROR(lseek(fd, 0, SEEK_SET));
      for (size_t i = 0; i < num_writes; ++i) {
        CHECK_ERROR(write(fd, buffer, kBufferSize));
      }
    }
    std::cerr << "> Done initializing the file." << std::endl;
  } else {
    std::cerr << "> File is already large enough. Skipping initialization."
              << std::endl;
  }

  return fd;
}

auto RunBenchmark(void* buffer, const int fd) {
  std::mt19937 prng(FLAGS_seed + 1);
  const std::vector<size_t>& page_sizes = ([]() {
    if (FLAGS_mode == "all") {
      return kAllPages;
    } else if (FLAGS_mode == "power_two") {
      return kPowerTwoPages;
    } else if (FLAGS_mode == "single") {
      return kSingle;
    } else {
      throw std::runtime_error("Unknown mode.");
    }
  })();

  const size_t num_pages_in_file =
      FLAGS_size_mib * 1024ULL * 1024ULL / kPageSize;
  auto run_read_workload = [&page_sizes, &prng, buffer, num_pages_in_file,
                            fd](const uint32_t pages_to_read) {
    std::uniform_int_distribution<uint32_t> num_pages_dist(
        0, page_sizes.size() - 1);
    std::uniform_int_distribution<size_t> page_offset_dist(
        0, num_pages_in_file - page_sizes.back());
    // This is a rough benchmark since we don't make sure we hit `pages_to_read`
    // exactly.
    int64_t pages_left = pages_to_read;
    while (pages_left > 0) {
      const uint32_t num_pages = page_sizes[num_pages_dist(prng)];
      const size_t byte_offset = page_offset_dist(prng) * kPageSize;
      CHECK_ERROR(pread(fd, buffer, num_pages * kPageSize, byte_offset));
      pages_left -= num_pages;
    }
    // Returns the number of pages actually read.
    return -pages_left + pages_to_read;
  };

  std::cerr << "> Running warmup: " << FLAGS_warmup << " pages..." << std::endl;
  run_read_workload(FLAGS_warmup);

  std::cerr << "> Done warmup. Running workload: " << FLAGS_iterations
            << " pages..." << std::endl;
  const auto start = std::chrono::steady_clock::now();
  const int64_t pages_read = run_read_workload(FLAGS_iterations);
  const auto end = std::chrono::steady_clock::now();
  std::cerr << "Done workload." << std::endl;

  return std::make_pair(pages_read, end - start);
}

}  // namespace

int main(int argc, char* argv[]) {
  // This does not completely simulate the I/O pattern used by page grouping.
  // But we should start with something simple (and low effort to implement).
  // This program makes random reads of different lengths to the same file.
  gflags::SetUsageMessage("Run a randomized read microbenchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_file.empty() || FLAGS_out_path.empty()) {
    std::cerr << "ERROR: --file and --out_path both must be set." << std::endl;
    return 1;
  }

  void* const buffer = aligned_alloc(kAlignment, kBufferSize);
  assert(buffer != nullptr);

  const int file_fd = OpenAndInitializeFile(buffer);

  const auto out_dir = fs::path(FLAGS_out_path);
  std::ofstream out(out_dir / "results.csv");

  out << "pages_read,elapsed_time_ns" << std::endl;
  for (uint32_t i = 0; i < FLAGS_trials; ++i) {
    const auto [pages_read, time_ns] = RunBenchmark(buffer, file_fd);
    out << pages_read << "," << time_ns.count() << std::endl;
  }

  close(file_fd);
  free(buffer);
  return 0;
}
