#include "manager.h"

#include <fstream>
#include <iostream>
#include <limits>

#include "bufmgr/page_memory_allocator.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "segment_builder.h"
#include "util/key.h"

using namespace llsm;
using namespace llsm::pg;
namespace fs = std::filesystem;

namespace {

const std::string kSegmentFilePrefix = "sf-";
const std::string kSegmentDetailCsvFileName = "loaded_segments.csv";

Status LoadIntoPage(PageBuffer& buf, size_t page_idx, Key lower, Key upper,
                    const std::vector<std::pair<Key, Slice>>& records,
                    size_t start_idx, size_t end_idx) {
  key_utils::IntKeyAsSlice lower_key(lower), upper_key(upper);
  pg::Page page(buf.get() + pg::Page::kSize * page_idx, lower_key.as<Slice>(),
                upper_key.as<Slice>());
  for (size_t i = start_idx; i < end_idx; ++i) {
    const auto& r = records[i];
    key_utils::IntKeyAsSlice key(r.first);
    const auto res = page.Put(key.as<Slice>(), r.second);
    if (!res.ok()) {
      return res;
    }
  }
  return Status::OK();
}

void PrintSegmentsAsCSV(std::ostream& out,
                        const std::vector<Segment>& segments) {
  out << "segment_page_count,num_records,model_slope,model_intercept"
      << std::endl;
  for (const auto& seg : segments) {
    out << seg.page_count << "," << (seg.end_idx - seg.start_idx) << ",";
    if (seg.model.has_value()) {
      out << seg.model->line().slope() << "," << seg.model->line().intercept()
          << std::endl;
    } else {
      out << "," << std::endl;
    }
  }
}

}  // namespace

namespace llsm {
namespace pg {

Manager Manager::BulkLoadIntoSegments(
    const fs::path& db_path, const std::vector<std::pair<Key, Slice>>& records,
    const Manager::LoadOptions& options) {
  assert(options.use_segments);

  std::vector<SegmentFile> segment_files;
  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    segment_files.emplace_back(
        db_path / (kSegmentFilePrefix + std::to_string(i)),
        /*use_direct_io=*/true,
        /*initial_num_pages=*/1);
  }
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  PageBuffer buf = PageMemoryAllocator::Allocate(
      /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back());

  // 1. Generate the segments.
  SegmentBuilder builder(options.records_per_page_goal,
                         options.records_per_page_delta);
  const auto segments = builder.Build(records);
  if (options.print_segment_details) {
    std::ofstream segment_details(db_path / kSegmentDetailCsvFileName);
    PrintSegmentsAsCSV(segment_details, segments);
  }

  // 2. Load the data into pages on disk.
  for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
    const auto& seg = segments[seg_idx];
    const Key base_key = records[seg.start_idx].first;

    // 1. Build the segment in memory.
    memset(buf.get(), 0, pg::Page::kSize * seg.page_count);
    if (seg.page_count > 1) {
      // Partition the records into pages based on the model.
      size_t curr_page = 0;
      size_t curr_page_first_record_idx = seg.start_idx;
      for (size_t i = seg.start_idx; i < seg.end_idx; ++i) {
        const auto& rec = records[i];
        // Use the page assigned by the model.
        const size_t assigned_page =
            std::min(seg.page_count - 1,
                     static_cast<size_t>(std::max(
                         0.0, seg.model->line()(rec.first - base_key))));
        if (assigned_page != curr_page) {
          // Flush to page.
          const auto result = LoadIntoPage(
              buf, curr_page,
              /*lower_key=*/records[curr_page_first_record_idx].first,
              /*upper_key=*/rec.first, records,
              /*start_idx=*/curr_page_first_record_idx, /*end_idx=*/i);
          assert(result.ok());
          curr_page = assigned_page;
          curr_page_first_record_idx = i;
        }
      }
      // Flush remaining to a page.
      const auto result =
          LoadIntoPage(buf, curr_page,
                       /*lower=*/records[curr_page_first_record_idx].first,
                       /*upper=*/
                       (seg_idx == segments.size() - 1)
                           // Max key if this is the last segment.
                           ? std::numeric_limits<uint64_t>::max()
                           // Otherwise, the next segment's first key.
                           : records[segments[seg_idx + 1].start_idx].first,
                       records, curr_page_first_record_idx, records.size());
      assert(result.ok());

      // Write model into the first page (for deserialization).
      pg::Page first_page(buf.get());
      first_page.SetModel(seg.model->line());

    } else {
      // Simple case - put all the records into one page.
      assert(seg.page_count == 1);
      const auto result = LoadIntoPage(
          buf, 0, /*lower=*/records[seg.start_idx].first, /*upper=*/
          (seg_idx == segments.size() - 1)
              // Max key if this is the first segment.
              ? std::numeric_limits<uint64_t>::max()
              // Otherwise, the next segment's first key.
              : records[segments[seg_idx + 1].start_idx].first,
          records, seg.start_idx, seg.end_idx);
      assert(result.ok());
    }

    // 2. Write it to disk.
    const size_t segment_idx =
        SegmentBuilder::kPageCountToSegment.find(seg.page_count)->second;
    SegmentFile& sf = segment_files[segment_idx];
    const size_t byte_offset = sf.AllocateSegment();
    sf.WritePages(byte_offset, buf.get(), seg.page_count);

    // Record the segment boundary.
    SegmentId seg_id(/*file_offset=*/segment_idx,
                     /*page_offset=*/byte_offset / pg::Page::kSize);
    segment_boundaries.emplace_back(
        base_key, SegmentInfo(seg_id, seg.model.has_value()
                                          ? seg.model->line()
                                          : std::optional<plr::Line64>()));
  }

  return Manager(db_path, std::move(segment_boundaries),
                 std::move(segment_files));
}

Manager Manager::BulkLoadIntoPages(
    const fs::path& db, const std::vector<std::pair<Key, Slice>>& records,
    const Manager::LoadOptions& options) {
  PageBuffer buf = PageMemoryAllocator::Allocate(/*num_pages=*/1);
}

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<SegmentFile> segment_files)
    : db_path_(std::move(db_path)), segment_files_(std::move(segment_files)) {
  index_.bulk_load(boundaries.begin(), boundaries.end());
}

Manager Manager::LoadIntoNew(const fs::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const LoadOptions& options) {
  fs::create_directory(db);

  if (options.use_segments) {
    return BulkLoadIntoSegments(db, records, options);
  } else {
    return BulkLoadIntoPages(db, records, options);
  }
}

Manager Manager::Reopen(const fs::path& db) {}

Status Manager::Get(const Key& key, std::string* value_out) {
  return Status::OK();
}

Status Manager::PutBatch(const std::vector<std::pair<Key, Slice>>& records) {
  return Status::OK();
}

Status Manager::Scan(const Key& start_key, const size_t amount,
                     std::vector<std::pair<Key, std::string>>* values_out) {
  return Status::OK();
}

}  // namespace pg
}  // namespace llsm
