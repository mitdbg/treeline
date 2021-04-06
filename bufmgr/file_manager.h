#pragma once

#include <filesystem>
#include <memory>
#include <mutex>

#include "bufmgr/file.h"
#include "bufmgr/logical_page_id.h"
#include "bufmgr/options.h"
#include "bufmgr/physical_page_id.h"
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

  // Reads the part of the on-disk database file corresponding to
  // `logical_page_id` into the in-memory page-sized block pointed to by `data`.
  void ReadPage(const LogicalPageId logical_page_id, void* data);

  // Writes from the in-memory page-sized block pointed to by `data` to the part
  // of the on-disk database file corresponding to `logical_page_id`.
  void WritePage(const LogicalPageId logical_page_id, void* data);

  // Allocates a new page as an overflow page and updates the overflow page
  // table accordingly. Returns the logical page id of the newly allocated page.
  LogicalPageId AllocatePage();

  // Provides the total number of pages currently used.
  size_t GetNumPages() const { return total_pages_; }

  // Provides the total number of segments currently used.
  size_t GetNumSegments() const { return total_segments_; }

 private:
  // Consult the correct page table to retrieve the PhysicalPageId for
  // `logical_page_id`.
  PhysicalPageId& GetPhysicalPageId(const LogicalPageId logical_page_id);

  // The database files
  std::vector<std::unique_ptr<File>> db_files_;

  // The path to the database
  const std::filesystem::path db_path_;

  // The PhysicalPageId corresponding to each raw page id for normal pages.
  std::vector<PhysicalPageId> page_table_;

  // The PhysicalPageId corresponding to each raw page id for overflow
  // pages.
  std::vector<PhysicalPageId> overflow_page_table_;

  // The total number of pages used.
  size_t total_pages_;

  // The total number of segments used.
  size_t total_segments_;

  // The next segment to expand with additional overflow pages.
  size_t next_segment_to_expand_;

  // A mutex on new page allocation
  std::mutex page_allocation_mutex_;
};

}  // namespace llsm
