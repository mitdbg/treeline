#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../bufmgr/page_memory_allocator.h"
#include "../../util/key.h"
#include "../manager.h"
#include "../persist/page.h"
#include "../persist/segment_file.h"
#include "../persist/segment_id.h"
#include "../persist/segment_wrap.h"
#include "../segment_builder.h"
#include "gflags/gflags.h"
#include "treeline/pg_options.h"

DEFINE_string(db_path, "", "Path to the database to check.");
DEFINE_bool(verbose, false,
            "If set to true, will print details about all errors.");
DEFINE_bool(stop_early, false,
            "If set, will abort later checks if earlier checks fail.");
DEFINE_bool(ensure_no_overflows, false,
            "If set, will return a non-zero exit code if the database has "
            "overflow pages.");

DEFINE_bool(scan_db, false,
            "Set to true to scan `scan_amount` records from the DB and to "
            "check the results for consistency.");
DEFINE_uint64(scan_amount, 100000000ULL, "The number of records to scan.");

namespace {

using namespace tl;
using namespace tl::pg;
namespace fs = std::filesystem;

struct SegmentState {
  SegmentId id;
  Key base_key;
  Key upper_bound;
  size_t page_count;
  bool checksum_valid;
  uint32_t sequence_number;
  std::vector<SegmentId> overflows;
};

struct FreeSegment {
  SegmentId id;
  size_t page_count;
};

void PrintSegmentBounds(std::ostream& out, const size_t idx,
                        const SegmentState& seg) {
  out << "Index: " << idx << " ID: " << seg.id << " Base Key: " << seg.base_key
      << " Upper Key: " << seg.upper_bound;
}

class DBState {
 public:
  static DBState Load(const fs::path& db_path);

  // Verify that the encoded key ranges in each segment are "valid".
  bool CheckSegmentRanges() const;

  // Check that overflows appear in single-page segment file only and that there
  // are no dangling overflows.
  bool CheckCorrectOverflows() const;

  // Returns false iff there exist overflow pages. This check is useful when you
  // want to ensure the DB is "flat".
  //
  // NOTE: This check should only run if `CheckCorrectOverflows()` passes (i.e.,
  // returns true). Otherwise this check's output is not meaningful, since there
  // exist corrupted overflow pages.
  bool CheckNoOverflows() const;

  // Check segment checksums.
  bool CheckChecksums() const;

  // Print a summary of the free segments in the DB.
  void PrintFreeSegmentsSummary(std::ostream& out) const;

  // Check the boundaries in each on-disk page.
  bool CheckPageRanges() const;

 private:
  DBState(fs::path db_path, bool uses_segments,
          std::unordered_set<SegmentId> declared_overflows,
          std::vector<SegmentState> segments,
          std::vector<FreeSegment> free_segments);

  fs::path db_path_;
  bool uses_segments_;
  // Pages that declare themselves as overflows.
  std::unordered_set<SegmentId> declared_overflows_;
  std::vector<SegmentState> segments_;
  std::vector<FreeSegment> free_segments_;
};

DBState DBState::Load(const fs::path& db_path) {
  // Figure out if there are segments in this DB.
  const bool uses_segments = fs::exists(db_path / "sf-1");

  PageBuffer page_buffer = PageMemoryAllocator::Allocate(
      /*num_pages=*/SegmentBuilder::SegmentPageCounts().back());
  void* buf = page_buffer.get();
  pg::Page first_page(buf);

  std::unordered_set<SegmentId> declared_overflows;
  std::vector<SegmentState> segments;
  std::vector<FreeSegment> free_segments;

  for (size_t i = 0; i < SegmentBuilder::SegmentPageCounts().size(); ++i) {
    if (i > 0 && !uses_segments) break;
    const size_t pages_per_segment = SegmentBuilder::SegmentPageCounts()[i];
    SegmentFile sf(db_path / ("sf-" + std::to_string(i)), pages_per_segment,
                   /*use_memory_based_io=*/true);

    const size_t num_segments = sf.NumAllocatedSegments();
    const size_t bytes_per_segment = pages_per_segment * pg::Page::kSize;

    std::cout << "Segment file " << i
              << " last allocated segment's page offset: "
              << (num_segments * pages_per_segment) << std::endl;

    for (size_t seg_idx = 0; seg_idx < num_segments; ++seg_idx) {
      // Offset in `id` is the page offset.
      SegmentId id(i, seg_idx * pages_per_segment);
      sf.ReadPages(seg_idx * bytes_per_segment, buf, pages_per_segment);
      SegmentWrap sw(buf, pages_per_segment);

      if (!first_page.IsValid()) {
        auto& fs = free_segments.emplace_back();
        fs.id = id;
        fs.page_count = pages_per_segment;
        continue;
      }

      // Check if any pages in the segment declare themselves as overflow pages.
      // In a consistent DB, overflows only exist in sf-0.
      sw.ForEachPage(
          [&declared_overflows, &id](const size_t idx, const pg::Page& page) {
            if (!page.IsOverflow()) return;
            const auto res = declared_overflows.insert(
                SegmentId(id.GetFileId(), id.GetOffset() + idx));
            assert(res.second);
          });

      // This is not a "regular" segment.
      if (pages_per_segment == 1 && first_page.IsOverflow()) {
        continue;
      }

      auto& seg = segments.emplace_back();
      seg.id = id;
      seg.base_key = sw.EncodedBaseKey();
      // The encoded upper key is always one less than the upper bound
      // (exclusive). This is because the on-disk page expects an inclusive
      // upper bound whereas we always use exclusive upper bounds in the page
      // grouping code.
      seg.upper_bound = sw.EncodedUpperKey() + 1;
      seg.checksum_valid = sw.CheckChecksum();
      seg.page_count = pages_per_segment;
      seg.sequence_number = sw.GetSequenceNumber();

      // Record overflow pointers.
      sw.ForEachPage([&seg](const size_t idx, const pg::Page& page) {
        if (!page.HasOverflow()) return;
        seg.overflows.push_back(page.GetOverflow());
      });
    }
  }

  return DBState(db_path, uses_segments, std::move(declared_overflows),
                 std::move(segments), std::move(free_segments));
}

DBState::DBState(fs::path db_path, bool uses_segments,
                 std::unordered_set<SegmentId> declared_overflows,
                 std::vector<SegmentState> segments,
                 std::vector<FreeSegment> free_segments)
    : db_path_(db_path),
      uses_segments_(uses_segments),
      declared_overflows_(std::move(declared_overflows)),
      segments_(std::move(segments)),
      free_segments_(std::move(free_segments)) {
  std::sort(segments_.begin(), segments_.end(),
            [](const auto& left, const auto& right) {
              return left.base_key < right.base_key;
            });
}

bool DBState::CheckSegmentRanges() const {
  assert(!segments_.empty());

  size_t internal_range_errors = 0;
  std::cout << std::endl << ">>> Checking segment ranges..." << std::endl;
  for (size_t i = 0; i < segments_.size(); ++i) {
    const auto& seg = segments_[i];
    if (seg.base_key >= seg.upper_bound) {
      ++internal_range_errors;
      if (FLAGS_verbose) {
        std::cout << "ERROR: Invalid internal segment range. ";
        PrintSegmentBounds(std::cout, i, seg);
        std::cout << std::endl;
      }
    }
  }

  size_t cross_segment_errors = 0;
  for (size_t i = 1; i < segments_.size(); ++i) {
    const auto& prev_seg = segments_[i - 1];
    const auto& curr_seg = segments_[i];
    if (curr_seg.base_key < prev_seg.upper_bound) {
      ++cross_segment_errors;
      if (FLAGS_verbose) {
        std::cout << "ERROR: Segment range overlap." << std::endl;
        std::cout << "Left: ";
        PrintSegmentBounds(std::cout, i - 1, prev_seg);
        std::cout << std::endl;
        std::cout << "Right: ";
        PrintSegmentBounds(std::cout, i, curr_seg);
        std::cout << std::endl;
      }
    }
  }

  std::cout << ">>> Internal segment range errors: " << internal_range_errors
            << "/" << segments_.size() << std::endl;
  std::cout << ">>> Cross segment range errors: " << cross_segment_errors << "/"
            << segments_.size() << std::endl;

  return internal_range_errors + cross_segment_errors == 0;
}

bool DBState::CheckCorrectOverflows() const {
  // Check that overflow pages are allocated in the correct file.
  std::cout << std::endl << ">>> Checking overflows..." << std::endl;
  size_t num_incorrect_overflows = 0;
  for (const auto& id : declared_overflows_) {
    if (id.GetFileId() != 0) {
      ++num_incorrect_overflows;
      if (FLAGS_verbose) {
        std::cout << "ERROR: Overflow page in incorrect file. ID: " << id
                  << std::endl;
      }
    }
  }

  // Check for dangling overflows.
  // - Every page that declares itself as an overflow should be referenced by an
  //   existing segment.
  // - Every overflow that is referenced by a segment should exist.

  // Store the number of times the overflow page is referenced. In consistent
  // DBs, all overflow pages should be referenced exactly once.
  std::unordered_map<SegmentId, size_t> overflow_refs;
  overflow_refs.reserve(declared_overflows_.size());
  for (const auto& id : declared_overflows_) {
    overflow_refs[id] = 0;
  }

  // Check segments first.
  size_t pointing_to_nonexistent_overflow = 0;
  size_t multiple_references = 0;
  for (size_t i = 0; i < segments_.size(); ++i) {
    const auto& seg = segments_[i];
    for (size_t j = 0; j < seg.overflows.size(); ++j) {
      const auto& overflow_id = seg.overflows[j];
      const auto it = overflow_refs.find(overflow_id);

      // Segment page points to unknown overflow page.
      if (it == overflow_refs.end()) {
        ++pointing_to_nonexistent_overflow;
        if (FLAGS_verbose) {
          std::cout
              << "ERROR: Page " << j << " in segment " << seg.id
              << " references a non-existent overflow page (references ID: "
              << overflow_id << ")" << std::endl;
        }
        continue;
      }

      // Multiple segment pages point to the same overflow page.
      if (it->second > 0) {
        ++multiple_references;
        if (FLAGS_verbose) {
          std::cout << "ERROR: Page " << j << " in segment " << seg.id
                    << " is not the first page that references overflow page "
                    << overflow_id << std::endl;
        }
      }

      // Increase the reference count.
      ++(it->second);
    }
  }

  // Ensure that every declared overflow page was referenced once. We do not
  // warn on multiple references here because we check for that above.
  size_t unreferenced_overflows = 0;
  for (const auto& [seg_id, ref_count] : overflow_refs) {
    if (ref_count == 0) {
      ++unreferenced_overflows;
      if (FLAGS_verbose) {
        std::cout << "ERROR: Overflow page " << seg_id
                  << " was not referenced by any existing segment page."
                  << std::endl;
      }
    }
  }

  std::cout << ">>> Incorrect overflow allocations: " << num_incorrect_overflows
            << "/" << declared_overflows_.size() << std::endl;
  std::cout << ">>> Segment pages that point to non-existing overflows: "
            << pointing_to_nonexistent_overflow << std::endl;
  std::cout << ">>> Overflow pages referenced multiple times: "
            << multiple_references << std::endl;
  std::cout << ">>> Overflow pages never referenced: " << unreferenced_overflows
            << std::endl;

  return num_incorrect_overflows + pointing_to_nonexistent_overflow +
             multiple_references + unreferenced_overflows ==
         0;
}

bool DBState::CheckNoOverflows() const { return declared_overflows_.empty(); }

bool DBState::CheckChecksums() const {
  size_t invalid_checksums = 0;
  std::cout << std::endl << ">>> Checking segment checksums..." << std::endl;
  for (size_t i = 0; i < segments_.size(); ++i) {
    const auto& seg = segments_[i];
    if (!seg.checksum_valid) {
      ++invalid_checksums;
      if (FLAGS_verbose) {
        std::cout << "ERROR: Invalid checksum. Index: " << i
                  << " ID: " << seg.id << std::endl;
      }
    }
  }
  std::cout << ">>> Invalid checksums: " << invalid_checksums << "/"
            << segments_.size() << std::endl;

  return invalid_checksums == 0;
}

void DBState::PrintFreeSegmentsSummary(std::ostream& out) const {
  // File ID -> Number of free segments
  std::unordered_map<size_t, size_t> free_counts;
  for (const auto& f : free_segments_) {
    ++(free_counts[f.id.GetFileId()]);
  }

  out << std::endl << ">>> Free segments summary" << std::endl;
  for (size_t i = 0; i < SegmentBuilder::SegmentPageCounts().size(); ++i) {
    out << "Length " << SegmentBuilder::SegmentPageCounts()[i] << ": "
        << free_counts[i] << std::endl;
  }
}

bool DBState::CheckPageRanges() const {
  std::cout << std::endl << ">>> Checking page ranges..." << std::endl;

  PageBuffer page_buffer = PageMemoryAllocator::Allocate(
      /*num_pages=*/SegmentBuilder::SegmentPageCounts().back());
  void* const buf = page_buffer.get();

  std::vector<std::unique_ptr<SegmentFile>> segment_files;
  segment_files.reserve(SegmentBuilder::SegmentPageCounts().size());
  for (size_t i = 0; i < SegmentBuilder::SegmentPageCounts().size(); ++i) {
    if (i > 0 && !uses_segments_) break;
    const size_t pages_per_segment = SegmentBuilder::SegmentPageCounts()[i];
    segment_files.push_back(std::make_unique<SegmentFile>(
        db_path_ / ("sf-" + std::to_string(i)), pages_per_segment,
        /*use_memory_based_io=*/true));
  }

  size_t total_pages = 0;
  // Number of pages with invalid lower/upper boundaries.
  size_t invalid_key_bounds = 0;
  // Number of pages with boundaries that are outside the segment's boundaries.
  size_t invalid_bounds_for_segment = 0;
  // Number of pages that contain keys that do not fall within their lower/upper
  // boundaries.
  size_t pages_with_incorrect_keys = 0;
  // Number of pages whose lower bound does not match the previous page's upper
  // bound (i.e., the pages leave a gap in the key space).
  size_t pages_leaving_gaps = 0;

  for (size_t i = 0; i < segments_.size(); ++i) {
    const auto& seg = segments_[i];
    if (!seg.checksum_valid) {
      // Skip invalid segments.
      continue;
    }
    const auto& sf = segment_files[seg.id.GetFileId()];
    sf->ReadPages(seg.id.GetOffset() * pg::Page::kSize, buf, seg.page_count);
    total_pages += seg.page_count;
    total_pages += seg.overflows.size();

    SegmentWrap sw(buf, seg.page_count);
    Key prev_upper;
    const auto check_page = [&seg, &invalid_key_bounds,
                             &pages_with_incorrect_keys,
                             &invalid_bounds_for_segment, &prev_upper,
                             &pages_leaving_gaps](const size_t idx,
                                                  const pg::Page& page) {
      // Check page bounds.
      const Key lower = key_utils::ExtractHead64(page.GetLowerBoundary());
      const Key upper = key_utils::ExtractHead64(page.GetUpperBoundary());
      if (lower > upper) {
        ++invalid_key_bounds;
        if (FLAGS_verbose) {
          std::cout << "ERROR: Page " << idx << " in segment " << seg.id
                    << " has invalid key boundaries. Lower: " << lower
                    << " Upper: " << upper << std::endl;
        }
        // Do not do further validation on this page.
        return;
      }

      // Check that the page falls within the segment.
      if (lower < seg.base_key || upper >= seg.upper_bound) {
        ++invalid_bounds_for_segment;
        if (FLAGS_verbose) {
          std::cout << "ERROR: Page " << idx << " in segment " << seg.id
                    << " has key boundaries that exceed the segment's "
                       "boundaries. Lower: "
                    << lower << " Upper: " << upper
                    << " Segment base: " << seg.base_key
                    << " Segment upper: " << seg.upper_bound << std::endl;
        }
      }

      // Check that the records in the segment fall within the lower/upper
      // boundaries.
      for (auto it = page.GetIterator(); it.Valid(); it.Next()) {
        const Key k = key_utils::ExtractHead64(page.GetLowerBoundary());
        if (k < lower || k > upper) {
          ++pages_with_incorrect_keys;
          if (FLAGS_verbose) {
            std::cout << "ERROR: Page " << idx << " in segment " << seg.id
                      << " has a key that falls outside its boundaries. Lower: "
                      << lower << " Upper: " << upper << " Key: " << k
                      << std::endl;
          }
          break;
        }
      }

      // Check that this page's lower bound matches the previous page's upper
      // bound. Note that the encoded page boundaries are inclusive.
      //
      // We only do this check for indexes in the exclusive range (0, 16). Page
      // 0 is the first page in the segment. Page 16 and above represent
      // overflow pages (the index is not meaningful).
      if (idx > 0 && idx < 16) {
        if (prev_upper + 1 != lower) {
          ++pages_leaving_gaps;
          if (FLAGS_verbose) {
            std::cout << "ERROR: Page " << idx << " in segment " << seg.id
                      << " leaves a gap in the key space. Prev upper: "
                      << prev_upper << " Page lower: " << lower << std::endl;
          }
        }
      }

      prev_upper = upper;
    };

    sw.ForEachPage(check_page);

    // Check overflows.
    for (size_t j = 0; j < seg.overflows.size(); ++j) {
      SegmentId overflow = seg.overflows[j];
      const auto& osf = segment_files[overflow.GetFileId()];
      osf->ReadPages(overflow.GetOffset() * pg::Page::kSize, buf,
                     /*num_pages=*/1);
      pg::Page opage(buf);
      // HACK: We use at 16 and above to indicate overflow pages; the index is
      // not meaningful.
      check_page(j + 16, opage);
    }
  }

  std::cout << ">>> Pages with invalid key bounds: " << invalid_key_bounds
            << "/" << total_pages << std::endl;
  std::cout << ">>> Pages with invalid bounds for the segment: "
            << invalid_bounds_for_segment << "/" << total_pages << std::endl;
  std::cout << ">>> Pages with incorrect keys: " << pages_with_incorrect_keys
            << "/" << total_pages << std::endl;
  std::cout << ">>> Pages that leave gaps in the key space: "
            << pages_leaving_gaps << "/" << total_pages << std::endl;

  return invalid_key_bounds + invalid_bounds_for_segment +
             pages_with_incorrect_keys + pages_leaving_gaps ==
         0;
}

// Run a "fsck-like" validation over the DB's physical files.
bool RunCheck() {
  std::cout << ">>> Checking the DB's physical files for consistency."
            << std::endl;
  std::cout << ">>> Reading the DB files..." << std::endl;
  DBState db = DBState::Load(FLAGS_db_path);

  bool valid = true;
  valid = db.CheckChecksums() && valid;
  if (FLAGS_stop_early && !valid) return valid;

  valid = db.CheckSegmentRanges() && valid;
  if (FLAGS_stop_early && !valid) return valid;

  valid = db.CheckCorrectOverflows() && valid;
  if (FLAGS_stop_early && !valid) return valid;

  bool num_overflows_ok = true;
  if (FLAGS_ensure_no_overflows && valid) {
    num_overflows_ok = db.CheckNoOverflows();
  }

  db.PrintFreeSegmentsSummary(std::cout);

  valid = db.CheckPageRanges() && valid;

  if (valid) {
    std::cout << std::endl
              << ">>> ✓ On-disk database files are consistent." << std::endl;
  } else {
    std::cout << std::endl
              << ">>> ✗ Detected errors in the on-disk files." << std::endl;
  }

  if (!num_overflows_ok) {
    std::cout << ">>> ✗ The DB contains overflow pages but "
                 "--ensure_no_overflows was set."
              << std::endl;
  }

  return valid && num_overflows_ok;
}

// Attempt to scan "all" records from the DB and then check that the scanned
// records are unique and in sorted order.
bool RunScan() {
  std::cout << ">>> Scanning up to " << FLAGS_scan_amount
            << " records from the DB." << std::endl;
  PageGroupedDBOptions options;
  options.use_memory_based_io = true;
  std::cout << ">>> Opening the DB..." << std::endl;
  Manager m = Manager::Reopen(FLAGS_db_path, options);

  std::vector<std::pair<Key, std::string>> scanned_records;
  std::cout << ">>> Running the scan..." << std::endl;
  m.Scan(0, FLAGS_scan_amount, &scanned_records);

  // Report the number of scanned keys and check that (i) they are sorted, and
  // (ii) there are no duplicates.
  std::cout << ">>> Scanned " << scanned_records.size() << " records."
            << std::endl;
  if (scanned_records.size() == 0) {
    std::cout << std::endl << ">>> ✓ No records to check." << std::endl;
    return true;
  }

  std::cout << ">>> Checking scanned records for correctness..." << std::endl;
  bool correct = true;
  Key last_key = scanned_records[0].first;
  for (size_t i = 1; i < scanned_records.size(); ++i) {
    const Key key = scanned_records[i].first;
    if (key <= last_key) {
      std::cout << "ERROR: Out of order or duplicate key detected at index "
                << i << ". Previous key: " << last_key << " This key: " << key
                << std::endl;
      std::cout << std::endl
                << ">>> ✗ Detected errors in the scanned records." << std::endl;
      return false;
    }
    last_key = key;
  }

  std::cout
      << std::endl
      << ">>> ✓ Records were returned in sorted order. No duplicates detected."
      << std::endl;

  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Check a page grouped DB's on-disk files for consistency (fsck for "
      "page-grouped TreeLine).");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_db_path.empty()) {
    std::cerr << "ERROR: Must provide a path to an existing DB." << std::endl;
    return 1;
  }

  if (!FLAGS_scan_db) {
    return RunCheck() ? 0 : 2;
  } else {
    return RunScan() ? 0 : 3;
  }
}
