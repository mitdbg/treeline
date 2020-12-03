#include "file_manager.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

namespace llsm {

// Creates a file manager for `page_size`-sized pages by opening a database
// file. Bypasses file system cache if `use_direct_io` is true.
FileManager::FileManager(const size_t page_size)
    : FileManager(page_size, /*use_direct_io = */ false) {}
FileManager::FileManager(const size_t page_size, const bool use_direct_io)
    : page_size_(page_size), next_page_id_(0) {
  db_fd_ = open(kFileName, O_CREAT | O_RDWR | (use_direct_io ? O_DIRECT : 0),
                S_IREAD | S_IWRITE);

  if (db_fd_ < 0) perror("Database file could not be opened");

  ZeroOut();
}

// Closes a database file.
FileManager::~FileManager() { close(db_fd_); }

// Reads the part of the on-disk database file corresponding to `page_id` into
// the in-memory page-sized block pointed to by `data`.
void FileManager::ReadPage(const uint64_t page_id, void* data) {
  while (page_id >= next_page_id_) ZeroOut();
  size_t offset = page_id * page_size_;
  ssize_t ret = pread(db_fd_, data, page_size_, offset);
  if (ret < 0) perror("ReadPage failed");
}

// Writes from the in-memory page-sized block pointed to by `data` to the part
// of the on-disk database file corresponding to `page_id`.
void FileManager::WritePage(const uint64_t page_id, void* data) {
  while (page_id >= next_page_id_) ZeroOut();
  size_t offset = page_id * page_size_;
  ssize_t ret = pwrite(db_fd_, data, page_size_, offset);
  if (ret < 0) perror("WritePage failed");
}

// Fill kZeroOutPages sequential pages with zeroes, starting with next_page_id_.
// Update next_page_id_ accordingly.
void FileManager::ZeroOut() {
  next_page_id_ += kZeroOutPages;
  size_t new_total_length = next_page_id_ * page_size_;

  int ret = ftruncate(db_fd_, new_total_length);
  if (ret < 0) perror("ZeroOut failed");
}

}  // namespace llsm
