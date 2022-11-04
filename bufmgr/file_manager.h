#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

#include "bufmgr/file.h"
#include "bufmgr/options.h"
#include "db/page.h"

namespace tl {

// A wrapper for I/O to on-disk files.
//
// This class helps make `BufferManager` OS-independent.
class FileManager {
 public:
  // Creates a file manager according to the options specified in `options`.
  FileManager(const BufMgrOptions& options, std::filesystem::path db_path);

  // Reads the part of the on-disk database file corresponding to
  // `physical_page_id` into the in-memory page-sized block pointed to by
  // `data`. Returns Status::OK() on success and Status::InvalidArgument() if
  // `physical_page_id` does not correspond to an already-allocated page.
  Status ReadPage(const PhysicalPageId physical_page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `physical_page_id`. Returns
  // Status::OK() on success and Status::InvalidArgument() if `physical_page_id`
  // does not correspond to an already-allocated page.
  Status WritePage(const PhysicalPageId physical_page_id, void* data);

  // Allocates a new page and returns the page id.
  PhysicalPageId AllocatePage();

  // Provides the total number of pages currently used.
  size_t GetNumPages() const { return total_pages_; }

  // Provides the total number of segments currently used.
  size_t GetNumSegments() const { return total_segments_; }

 private:
  // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  const std::filesystem::path db_path_;

  // The total number of pages used.
  size_t total_pages_;

  // The total number of segments used.
  size_t total_segments_;

  // The next segment to expand with additional overflow pages.
  size_t next_segment_to_expand_;

  // A mutex on new page allocation
  std::mutex page_allocation_mutex_;
};

}  // namespace tl
