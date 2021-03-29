#pragma once

#include <filesystem>
#include <memory>

#include "bufmgr/file.h"
#include "bufmgr/file_address.h"
#include "bufmgr/options.h"
#include "db/page.h"
#include "model/model.h"

namespace llsm {

// A wrapper for I/O to on-disk files.
//
// This class helps make `BufferManager` OS-independent.
class FileManager {
 public:
  // Creates a file manager according to the options specified in `options`.
  FileManager(const BufMgrOptions& options, std::filesystem::path db_path);

  // Reads the part of the on-disk database file corresponding to `page_id` into
  // the in-memory page-sized block pointed to by `data`.
  void ReadPage(const uint64_t page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `page_id`.
  void WritePage(const uint64_t page_id, void* data);

  // Provides the total number of pages currently used.
  size_t GetNumPages() const { return total_pages_; }

  // Provides the total number of segments currently used.
  size_t GetNumSegments() const { return total_segments_; }

 private:
  // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  const std::filesystem::path db_path_;

  // The FileAddress corresponding to each page_id.
  std::vector<FileAddress> page_table_;

  // The total number of pages used.
  size_t total_pages_;

  // The total number of segments used.
  size_t total_segments_;
};

}  // namespace llsm
