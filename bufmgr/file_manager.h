#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <atomic>

namespace llsm {
// Manages I/O to on-disk files on behalf of BufferManager.
class FileManager {
  // The number of pages to zero out whenever ZeroOut() is called
  static const size_t kZeroOutPages = 256;

 public:
  // Creates a file manager for `page_size`-sized pages by opening a database
  // file. Bypasses file system cache if `use_direct_io` is true.
  FileManager(const size_t page_size);
  FileManager(const size_t page_size, const bool use_direct_io);

  // Closes a database file.
  ~FileManager();

  // Reads the part of the on-disk database file corresponding to `page_id` into
  // the in-memory page-sized block pointed to by `data`.
  void ReadPage(const uint64_t page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `page_id`.
  void WritePage(const uint64_t page_id, void* data);

 private:
  // Fill kZeroOutPages sequential pages with zeroes, starting with
  // next_page_id_. Update next_page_id_ accordingly at the end.
  void ZeroOut();

  // The file descriptor of the database file
  int database_fd_;

  // The size of a page (in bytes)
  size_t page_size_;

  // The lowest page id that has never been written to
  size_t next_page_id_;
};

}  // namespace llsm
