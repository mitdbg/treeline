#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "llsm/options.h"

#define CHECK_ERROR(call)                                                    \
  do {                                                                       \
    if ((call) < 0) {                                                        \
      const char* error = strerror(errno);                                   \
      std::cerr << __FILE__ << ":" << __LINE__ << " " << error << std::endl; \
      throw std::runtime_error(std::string(error));                          \
    }                                                                        \
  } while (0)

namespace llsm {
class File {
 public:
  File(const BufMgrOptions options, const std::string& name)
      : fd_(-1),
        page_size_(options.page_size),
        max_offset_written_(0),
        growth_bytes_(options.growth_pages * page_size_) {
    CHECK_ERROR(fd_ = open(name.c_str(),
                           O_CREAT | O_RDWR | O_SYNC |
                               (options.use_direct_io ? O_DIRECT : 0),
                           S_IRUSR | S_IWUSR));
  }
  ~File() { close(fd_); }
  void ReadPage(size_t offset, void* data) const {
    CHECK_ERROR(pread(fd_, data, page_size_, offset));
  }
  void WritePage(size_t offset, const void* data) const {
    CHECK_ERROR(pwrite(fd_, data, page_size_, offset));
  }
  void Sync() const { fsync(fd_); }

  void ZeroOut(size_t offset) {
    if (max_offset_written_ > offset + page_size_) return;

    while (max_offset_written_ < offset + page_size_)
      max_offset_written_ += growth_bytes_;

    CHECK_ERROR(ftruncate(fd_, max_offset_written_));
  }
  const size_t page_size_;

 private:
  int fd_;
  size_t max_offset_written_;
  const size_t growth_bytes_;
};

}  // namespace llsm