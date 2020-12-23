#pragma once

#include <memory>

#include "db/page.h"
#include "file.h"

namespace llsm {

class File;

// A wrapper for I/O to on-disk files.
//
// This class helps make `BufferManager` OS-independent.
class FileManager {
 public:
  // Creates a file manager according to the options specified in `options`.
  FileManager(const BufMgrOptions options, std::string db_path);

  // Reads the part of the on-disk database file corresponding to `page_id` into
  // the in-memory page-sized block pointed to by `data`.
  void ReadPage(const uint64_t page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `page_id`.
  void WritePage(const uint64_t page_id, void* data);

 private:
  // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  std::string db_path_;

  // The number of bits in the page_id that index *within* the same segment.
  // i.e. this is 4 if there are 16 pages per segment.
  size_t pages_per_segment_bits_ = 0;

  // A given bit of this mask is 1 iff it indexes *within* the same segment.
  // i.e. this is 000...01111 if there are 16 pages per segment.
  size_t pages_per_segment_mask_ = 0;
};

}  // namespace llsm
