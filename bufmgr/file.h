#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <string>

#include "bufmgr/page_memory_allocator.h"
#include "db/page.h"
#include "treeline/status.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      exit(1);                                                               \
    }                                                                        \
  } while (0)

namespace tl {

class File {
  // The number of pages by which to grow a file when needed.
  const size_t kGrowthPages = 256;

 public:
  File(const std::filesystem::path& name, bool use_direct_io,
       size_t initial_num_pages)
      : fd_(-1),
        file_size_(0),
        growth_bytes_(kGrowthPages * Page::kSize),
        next_page_allocation_offset_(initial_num_pages * Page::kSize) {
    CHECK_ERROR(
        fd_ = open(name.c_str(),
                   O_CREAT | O_RDWR | O_SYNC | (use_direct_io ? O_DIRECT : 0),
                   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));

    // Retrieve the file's size.
    struct stat file_status;
    CHECK_ERROR(fstat(fd_, &file_status));
    assert(file_status.st_size >= 0);
    file_size_ = file_status.st_size;

    // If file already existed, retrieve next_page_allocation_offset_.
    if (file_size_ > 0) {
      PageBuffer page_data = PageMemoryAllocator::Allocate(/*num_pages=*/1);
      Page temp_page(page_data.get());
      next_page_allocation_offset_ = file_size_;
      for (size_t offset = 0; offset < file_size_; offset += Page::kSize) {
        ReadPage(offset, page_data.get());
        if (!temp_page.IsValid()) {
          next_page_allocation_offset_ = offset;
          break;
        }
      }
    }
  }
  ~File() { close(fd_); }
  Status ReadPage(size_t offset, void* data) const {
    if (offset >= next_page_allocation_offset_) {
      return Status::InvalidArgument("Tried to read from unallocated page.");
    }
    CHECK_ERROR(pread(fd_, data, Page::kSize, offset));
    return Status::OK();
  }
  Status WritePage(size_t offset, const void* data) const {
    if (offset >= next_page_allocation_offset_) {
      return Status::InvalidArgument("Tried to write to unallocated page.");
    }
    CHECK_ERROR(pwrite(fd_, data, Page::kSize, offset));
    return Status::OK();
  }
  void Sync() const { CHECK_ERROR(fsync(fd_)); }

  // Reserves space for an additional page on the file. This might involve
  // growing the file if needed, otherwise it just updates the bookkeeping.
  //
  // Returns the offset of the newly allocated page.
  size_t AllocatePage() {
    size_t allocated_offset = next_page_allocation_offset_;
    next_page_allocation_offset_ += Page::kSize;

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
      bytes_to_add += growth_bytes_;

    // Ensures the byte range [offset, offset + len) in the file has been
    // allocated (allocate it and zero it out if not).
    CHECK_ERROR(fallocate(fd_, /*mode=*/0, /*offset=*/file_size_,
                          /*len=*/bytes_to_add));
    file_size_ += bytes_to_add;
  }
  int fd_;
  size_t file_size_;
  const size_t growth_bytes_;
  size_t next_page_allocation_offset_;
};

}  // namespace tl
