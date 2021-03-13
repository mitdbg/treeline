#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <string>

#include "db/page.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      exit(1);                                                               \
    }                                                                        \
  } while (0)

namespace llsm {

class File {
  // The number of pages by which to grow a file when needed.
  const size_t kGrowthPages = 256;

 public:
  File(const std::filesystem::path& name, bool use_direct_io)
      : fd_(-1), file_size_(0), growth_bytes_(kGrowthPages * Page::kSize) {
    CHECK_ERROR(
        fd_ = open(name.c_str(),
                   O_CREAT | O_RDWR | O_SYNC | (use_direct_io ? O_DIRECT : 0),
                   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));

    // Retrieve the file's size.
    struct stat file_status;
    CHECK_ERROR(fstat(fd_, &file_status));
    assert(file_status.st_size >= 0);
    file_size_ = file_status.st_size;
  }
  ~File() { close(fd_); }
  void ReadPage(size_t offset, void* data) const {
    CHECK_ERROR(pread(fd_, data, Page::kSize, offset));
  }
  void WritePage(size_t offset, const void* data) const {
    CHECK_ERROR(pwrite(fd_, data, Page::kSize, offset));
  }
  void Sync() const { CHECK_ERROR(fsync(fd_)); }

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

 private:
  int fd_;
  size_t file_size_;
  const size_t growth_bytes_;
};

}  // namespace llsm
