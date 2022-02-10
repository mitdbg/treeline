#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
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

  void CheckSegmentRanges() const;

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

void DBState::CheckSegmentRanges() const {
  assert(!segments_.empty());

  size_t internal_range_errors = 0;
  std::cout << ">>> Checking internal segment ranges:" << std::endl;
  for (size_t i = 0; i < segments_.size(); ++i) {
    const auto& seg = segments_[i];
    if (seg.base_key > seg.upper_bound) {
      std::cout << "ERROR: Invalid internal segment range. ";
      PrintSegmentBounds(std::cout, i, seg);
      std::cout << std::endl;
      ++internal_range_errors;
    }
  }
  std::cout << ">>> Done." << std::endl;

  size_t cross_segment_errors = 0;
  std::cout << ">>> Checking segment ranges:" << std::endl;
  for (size_t i = 1; i < segments_.size(); ++i) {
    const auto& prev_seg = segments_[i - 1];
    const auto& curr_seg = segments_[i];
    if (curr_seg.base_key < prev_seg.upper_bound) {
      std::cout << "ERROR: Segment range overlap." << std::endl;
      std::cout << "Left: ";
      PrintSegmentBounds(std::cout, i - 1, prev_seg);
      std::cout << std::endl;
      std::cout << "Right: ";
      PrintSegmentBounds(std::cout, i - 1, prev_seg);
      std::cout << std::endl;
      ++cross_segment_errors;
    }
  }
  std::cout << ">>> Done." << std::endl;

  std::cout << "Summary:" << std::endl;
  std::cout << "Internal segment range errors: " << internal_range_errors << "/" << segments_.size() << std::endl;
  std::cout << "Cross segment range errors: " << cross_segment_errors << "/" << segments_.size() << std::endl;
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
  std::cout << ">>> Done! Running validation..." << std::endl;
  db.CheckSegmentRanges();

  return 0;
}
