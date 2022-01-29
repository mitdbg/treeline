#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>

#include "gflags/gflags.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      exit(1);                                                               \
    }                                                                        \
  } while (0)

DEFINE_string(db_path, "", "Path to the database directory.");
DEFINE_uint32(seed, 42, "The seed to use for the PRNG (for reproducibility).");
DEFINE_uint64(page_size, 4096, "The page size.");

namespace fs = std::filesystem;

// Returns the "logical" size of the file.
size_t FindAllocatedSize(int fd, const size_t pages_per_segment) {
  // Retrieve the file's size.
  struct stat file_status;
  CHECK_ERROR(fstat(fd, &file_status));
  assert(file_status.st_size >= 0);

  const size_t file_size = file_status.st_size;
  if (file_size == 0) return 0;

  // This assumes that segments are allocated contiguously and there are no
  // "holes".
  const size_t bytes_per_segment = pages_per_segment * FLAGS_page_size;
  const size_t num_complete_segments = file_size / bytes_per_segment;
  assert(num_complete_segments > 0);

  size_t segment_idx = num_complete_segments - 1;
  size_t segment_offset = segment_idx * bytes_per_segment;

  std::unique_ptr<uint8_t[]> buffer_ptr(new uint8_t[FLAGS_page_size]);
  void* buffer = buffer_ptr.get();

  // Iterate backwards through the file until we find the first valid segment.
  while (true) {
    CHECK_ERROR(pread(fd, buffer, FLAGS_page_size, segment_offset));

    // Hacky heuristic for checking segment validity.
    const uint64_t head = *reinterpret_cast<const uint64_t*>(buffer);
    if (head != 0) {
      return segment_offset + bytes_per_segment;
    }

    if (segment_idx == 0) {
      assert(segment_offset == 0);
      return 0;
    } else {
      --segment_idx;
      segment_offset -= bytes_per_segment;
    }
  }
}

void Shuffle(const fs::path& file, const size_t pages_per_segment) {
  int fd = -1;
  CHECK_ERROR(fd = open(file.c_str(), O_RDWR));

  const size_t logical_size = FindAllocatedSize(fd, pages_per_segment);
  if (logical_size == 0) {
    std::cerr << "> File is empty." << std::endl;
    return;
  }

  const size_t segment_size_bytes = pages_per_segment * FLAGS_page_size;
  assert(logical_size % segment_size_bytes == 0);
  const size_t num_segments_in_file = logical_size / segment_size_bytes;

  std::unique_ptr<uint8_t[]> buffer_ptr(new uint8_t[segment_size_bytes * 2]);
  void* seg0 = buffer_ptr.get();
  void* seg1 = buffer_ptr.get() + segment_size_bytes;

  // Fisher-Yates shuffle at the granularity of segments.
  std::mt19937 prng(FLAGS_seed + pages_per_segment);
  for (size_t i = 0; i < num_segments_in_file; ++i) {
    if (i % 100000 == 0) {
      std::cerr << "> Progress: " << i << "/" << num_segments_in_file
                << std::endl;
    }
    std::uniform_int_distribution<size_t> dist(i, num_segments_in_file - 1);
    const size_t swap_idx = dist(prng);
    // Perform a swap.
    CHECK_ERROR(pread(fd, seg0, segment_size_bytes, i * segment_size_bytes));
    CHECK_ERROR(
        pread(fd, seg1, segment_size_bytes, swap_idx * segment_size_bytes));
    CHECK_ERROR(pwrite(fd, seg1, segment_size_bytes, i * segment_size_bytes));
    CHECK_ERROR(
        pwrite(fd, seg0, segment_size_bytes, swap_idx * segment_size_bytes));
  }

  CHECK_ERROR(fsync(fd));
  close(fd);
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Shuffles the location of on-disk pages in a page-grouped database.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  if (FLAGS_db_path.empty()) {
    std::cerr << "ERROR: Must provide a database path." << std::endl;
    return 1;
  }

  // NOTE: For ease of implementation, this tool does not make crash consistent
  // writes.

  fs::path db_path(FLAGS_db_path);
  assert(fs::exists(db_path));

  for (size_t i = 0; i < 5; ++i) {
    const fs::path segment_file = db_path / ("sf-" + std::to_string(i));
    if (!fs::exists(segment_file)) {
      std::cerr << "WARNING: Could not find " << segment_file.filename()
                << std::endl;
      continue;
    }

    const size_t pages_per_segment = 1ULL << i;
    std::cerr << "> Shuffling " << segment_file.filename() << "..."
              << std::endl;
    Shuffle(segment_file, pages_per_segment);
    std::cerr << "> Done!" << std::endl;
  }

  return 0;
}
