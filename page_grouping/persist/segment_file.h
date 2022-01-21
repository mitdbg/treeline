#pragma once

#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

#include "bufmgr/page_memory_allocator.h"
#include "llsm/status.h"
#include "page.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      exit(1);                                                               \
    }                                                                        \
  } while (0)

namespace llsm {
namespace pg {

// Adapted from db/file.h.
class SegmentFile {
  // The number of pages by which to grow a file when needed.
  const size_t kGrowthPages = 256;
  const size_t kGrowthBytes = kGrowthPages * Page::kSize;

 public:
  // Represents an invalid file.
  SegmentFile()
      : fd_(-1),
        file_size_(0),
        next_page_allocation_offset_(0),
        pages_per_segment_(0) {}

  SegmentFile(const std::filesystem::path& name, bool use_direct_io,
              size_t pages_per_segment)
      : fd_(-1),
        file_size_(0),
        next_page_allocation_offset_(0),
        pages_per_segment_(pages_per_segment) {
    assert(pages_per_segment > 0);
    CHECK_ERROR(
        fd_ = open(name.c_str(),
                   O_CREAT | O_RDWR | O_SYNC | (use_direct_io ? O_DIRECT : 0),
                   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));

    // Retrieve the file's size.
    struct stat file_status;
    CHECK_ERROR(fstat(fd_, &file_status));
    assert(file_status.st_size >= 0);
    file_size_ = file_status.st_size;

    // If the file already existed, compute next_page_allocation_offset_. Note
    // that this assumes that segments are allocated contiguously.
    // TODO: This approach is temporary and will change to accomodate recovery.
    if (file_size_ > 0) {
      PageBuffer page_data = PageMemoryAllocator::Allocate(/*num_pages=*/1);
      Page temp_page(page_data.get());

      const size_t bytes_per_segment = pages_per_segment_ * Page::kSize;
      const size_t num_complete_segments = file_size_ / bytes_per_segment;
      assert(num_complete_segments > 0);

      size_t segment_idx = num_complete_segments - 1;
      size_t segment_offset = segment_idx * bytes_per_segment;
      next_page_allocation_offset_ = file_size_;
      // Scan backwards through the file until we find the first valid segment.
      while (true) {
        ReadPages(segment_offset, page_data.get(), /*num_pages=*/1);
        if (temp_page.IsValid()) {
          // Next segment is where the next allocation offset should be.
          next_page_allocation_offset_ = segment_offset + bytes_per_segment;
          break;
        }
        if (segment_idx == 0) {
          assert(segment_offset == 0);
          next_page_allocation_offset_ = 0;
          break;
        } else {
          --segment_idx;
          segment_offset -= bytes_per_segment;
        }
      }
    }
  }

  ~SegmentFile() {
    if (fd_ < 0) {
      // A negative file descriptor signifies an invalid file.
      return;
    }
    close(fd_);
  }

  SegmentFile(const SegmentFile&) = delete;
  SegmentFile& operator=(const SegmentFile&) = delete;

  SegmentFile(SegmentFile&& other)
      : fd_(other.fd_),
        file_size_(other.file_size_),
        next_page_allocation_offset_(other.next_page_allocation_offset_),
        pages_per_segment_(other.pages_per_segment_) {
    other.fd_ = -1;
  }

  SegmentFile& operator=(SegmentFile&& other) {
    fd_ = other.fd_;
    file_size_ = other.file_size_;
    next_page_allocation_offset_ = other.next_page_allocation_offset_;
    pages_per_segment_ = other.pages_per_segment_;
    other.fd_ = -1;
    return *this;
  }

  // The number of allocated segments in this file.
  // TODO: This will change when we implement proper recovery.
  size_t NumSegments() const {
    return next_page_allocation_offset_ / (pages_per_segment_ * Page::kSize);
  }

  size_t PagesPerSegment() const {
    return pages_per_segment_;
  }

  Status ReadPages(size_t offset, void* data, size_t num_pages) const {
    if (offset >= next_page_allocation_offset_) {
      return Status::InvalidArgument("Tried to read from unallocated page.");
    }
    CHECK_ERROR(pread(fd_, data, Page::kSize * num_pages, offset));
    return Status::OK();
  }

  Status WritePages(size_t offset, const void* data, size_t num_pages) const {
    if (offset >= next_page_allocation_offset_) {
      return Status::InvalidArgument("Tried to write to unallocated page.");
    }
    CHECK_ERROR(pwrite(fd_, data, Page::kSize * num_pages, offset));
    return Status::OK();
  }

  void Sync() const { CHECK_ERROR(fsync(fd_)); }

  // Reserves space for an additional page on the file. This might involve
  // growing the file if needed, otherwise it just updates the bookkeeping.
  //
  // Returns the offset of the newly allocated page.
  size_t AllocateSegment() {
    size_t allocated_offset = next_page_allocation_offset_;
    next_page_allocation_offset_ += Page::kSize * pages_per_segment_;

    ExpandToIfNeeded(allocated_offset);
    return allocated_offset;
  }

 private:
  // Ensures that the underlying file is large enough to be able to read/write
  // `Page::kSize` bytes starting at the given `offset`.
  //
  // If the file is too small, this method will use `fallocate()` to expand the
  // file; the newly allocated space will be initialized to all zeros. If the
  // file is already large enough, this method will be a no-op.
  void ExpandToIfNeeded(size_t offset) {
    if (file_size_ >= (offset + Page::kSize)) return;

    size_t bytes_to_add = 0;
    while ((file_size_ + bytes_to_add) < (offset + Page::kSize))
      bytes_to_add += kGrowthBytes;

    // Ensures the byte range [offset, offset + len) in the file has been
    // allocated (allocate it and zero it out if not).
    CHECK_ERROR(fallocate(fd_, /*mode=*/0, /*offset=*/file_size_,
                          /*len=*/bytes_to_add));
    file_size_ += bytes_to_add;
  }
  int fd_;
  size_t file_size_;
  size_t next_page_allocation_offset_;
  size_t pages_per_segment_;
};

}  // namespace pg
}  // namespace llsm
