#include "bufmgr/page_memory_allocator.h"

#include <sys/statvfs.h>

namespace tl {

size_t PageMemoryAllocator::alignment_ = PageMemoryAllocator::kDefaultAlignment;

void PageMemoryAllocator::SetAlignmentFor(const std::filesystem::path& path) {
  struct statvfs fs_stats;
  if (statvfs(path.c_str(), &fs_stats) == 0) {
    // We want memory to be aligned to the file system's block size to support
    // efficient direct I/O.
    alignment_ = fs_stats.f_bsize;
  } else {
    // The system call failed; use the default alignment instead.
    alignment_ = PageMemoryAllocator::kDefaultAlignment;
  }
}

}  // namespace tl
