#pragma once

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <new>

#include "db/page.h"

namespace tl {
namespace detail {

class PageBufferDeleter {
 public:
  void operator()(char* buffer) { free(buffer); }
};

}  // namespace detail

// A unique pointer to an appropriately aligned memory buffer that can be used
// for page direct I/O.
using PageBuffer = std::unique_ptr<char[], detail::PageBufferDeleter>;

// Used to get appropriately aligned memory for use with direct I/O.
class PageMemoryAllocator {
 public:
  // Get a heap-allocated buffer that is large enough to hold `num_pages` pages.
  // The memory buffer will be zeroed out before it is returned.
  static PageBuffer Allocate(size_t num_pages);

  // Sets the memory alignment for future `PageBuffer`s returned by
  // `Allocate()`. See the comments above `kDefaultAlignment` for more
  // information.
  static void SetAlignmentFor(const std::filesystem::path& path);

 private:
  // To support efficient direct I/O, TreeLine needs to align its memory buffers
  // to the block size of the underlying file system. When `SetAlignmentFor()`
  // is called, it will attempt to automatically determine the file system's
  // block size for the given path. In the unlikely event that the method is
  // unable to find the block size, it will fall back to using this default
  // alignment.
  static constexpr size_t kDefaultAlignment = 4096;

  // The alignment used for `PageBuffer` allocations.
  static size_t alignment_;
};

// Additional implementation details follow.

inline PageBuffer PageMemoryAllocator::Allocate(const size_t num_pages) {
  const size_t buffer_size = num_pages * Page::kSize;
  void* const buffer =
      aligned_alloc(PageMemoryAllocator::alignment_, buffer_size);
  if (buffer == nullptr) {
    throw std::bad_alloc();
  }
  memset(buffer, 0, buffer_size);
  return PageBuffer(reinterpret_cast<char*>(buffer));
}

}  // namespace tl
