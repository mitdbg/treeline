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
      : fd_(-1),
        max_offset_written_(0),
        growth_bytes_(kGrowthPages * Page::kSize) {
    CHECK_ERROR(
        fd_ = open(name.c_str(),
                   O_CREAT | O_RDWR | O_SYNC | (use_direct_io ? O_DIRECT : 0),
                   S_IRUSR | S_IWUSR));
  }
  ~File() { close(fd_); }
  void ReadPage(size_t offset, void* data) const {
    CHECK_ERROR(pread(fd_, data, Page::kSize, offset));
  }
  void WritePage(size_t offset, const void* data) const {
    CHECK_ERROR(pwrite(fd_, data, Page::kSize, offset));
  }
  void Sync() const { CHECK_ERROR(fsync(fd_)); }

  void ZeroOut(size_t offset) {
    if (max_offset_written_ > offset + Page::kSize) return;

    while (max_offset_written_ < offset + Page::kSize)
      max_offset_written_ += growth_bytes_;

    CHECK_ERROR(ftruncate(fd_, max_offset_written_));
  }

 private:
  int fd_;
  size_t max_offset_written_;
  const size_t growth_bytes_;
};

}  // namespace llsm
