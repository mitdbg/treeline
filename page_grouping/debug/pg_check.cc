#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../bufmgr/page_memory_allocator.h"
#include "../persist/page.h"
#include "../persist/segment_file.h"
#include "../persist/segment_id.h"
#include "../persist/segment_wrap.h"
#include "../segment_builder.h"
#include "gflags/gflags.h"

DEFINE_string(db_path, "", "Path to the database to check.");
DEFINE_bool(verbose, false,
            "If set to true, will print details about all errors.");
DEFINE_bool(stop_early, false,
            "If set, will abort later checks if earlier checks fail.");

namespace {

using namespace llsm;
using namespace llsm::pg;
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
  bool CheckOverflows() const;

  // Check segment checksums.
  bool CheckChecksums() const;

  // Print a summary of the free segments in the DB.
  void PrintFreeSegmentsSummary(std::ostream& out) const;

 private:
  DBState(bool uses_segments, std::unordered_set<SegmentId> declared_overflows,
          std::vector<SegmentState> segments,
          std::vector<FreeSegment> free_segments);

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
      /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back());
  void* buf = page_buffer.get();
  pg::Page first_page(buf);

  std::unordered_set<SegmentId> declared_overflows;
  std::vector<SegmentState> segments;
  std::vector<FreeSegment> free_segments;

  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    if (i > 0 && !uses_segments) break;
    const size_t pages_per_segment = SegmentBuilder::kSegmentPageCounts[i];
    SegmentFile sf(db_path / ("sf-" + std::to_string(i)), pages_per_segment,
                   /*use_memory_based_io=*/true);

    const size_t num_segments = sf.NumAllocatedSegments();
    const size_t bytes_per_segment = pages_per_segment * pg::Page::kSize;

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
      seg.upper_bound = sw.EncodedUpperKey();
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

  return DBState(uses_segments, std::move(declared_overflows),
                 std::move(segments), std::move(free_segments));
}

DBState::DBState(bool uses_segments,
                 std::unordered_set<SegmentId> declared_overflows,
                 std::vector<SegmentState> segments,
                 std::vector<FreeSegment> free_segments)
    : uses_segments_(uses_segments),
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
    if (seg.base_key > seg.upper_bound) {
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
        PrintSegmentBounds(std::cout, i - 1, prev_seg);
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

bool DBState::CheckOverflows() const {
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
  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    out << "Length " << SegmentBuilder::kSegmentPageCounts[i] << ": "
        << free_counts[i] << std::endl;
  }
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

  std::cout << ">>> Reading the DB files..." << std::endl;
  DBState db = DBState::Load(FLAGS_db_path);

  bool valid = true;
  valid = db.CheckChecksums() && valid;
  if (FLAGS_stop_early && !valid) return 1;

  valid = db.CheckSegmentRanges() && valid;
  if (FLAGS_stop_early && !valid) return 1;

  valid = db.CheckOverflows() && valid;
  if (FLAGS_stop_early && !valid) return 1;

  db.PrintFreeSegmentsSummary(std::cout);

  return valid ? 0 : 1;
}
