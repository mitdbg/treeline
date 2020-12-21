#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <atomic>
#include <memory>
#include <vector>

#include "llsm/options.h"
#include "db/page.h"

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
  // the in-memory page-sized block pointed to by `page`.
  void ReadPage(const uint64_t page_id, Page* page);

  // Writes from the in-memory page-sized block pointed to by `page` to the part
  // of the on-disk database file corresponding to `page_id`.
  void WritePage(const uint64_t page_id, Page* page);

 private:
  // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  std::string db_path_;

  const size_t pages_per_segment_;
};

}  // namespace llsm
