#pragma once

#include <filesystem>
#include <memory>

#include "db/page.h"
#include "file.h"
#include "model/model.h"

namespace llsm {

class File;

// Stores the file and byte-offset within the file where a specific page can
// be found.
struct FileAddress {
  size_t file_id = 0;
  size_t offset = 0;
};

// A wrapper for I/O to on-disk files.
//
// This class helps make `BufferManager` OS-independent.
class FileManager {
 public:
  // Creates a file manager according to the options specified in `options`.
  FileManager(const Options& options, std::filesystem::path db_path);

  // Reads the part of the on-disk database file corresponding to `page_id` into
  // the in-memory page-sized block pointed to by `data`.
  void ReadPage(const uint64_t page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `page_id`.
  void WritePage(const uint64_t page_id, void* data);

  // Derives a FileAddress given a `page_id`.
  FileAddress PageIdToAddress(size_t page_id) const;

  // Provides the total number of pages currently used.
  size_t GetNumPages() const {return total_pages_;}

  // Provides the total number of segments currently used.
  size_t GetNumSegments() const {return db_files_.size();}

  // Provides the page_id of the first page of each segment.
  std::vector<size_t> GetPageAllocation() const {return page_allocation_;}

 private:
    // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  const std::filesystem::path db_path_;

  // The page_id of the first page of each segment.
  std::vector<size_t> page_allocation_;

  // The total number of pages used.
  size_t total_pages_;
};

}  // namespace llsm
