#include "manager.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "bufmgr/page_memory_allocator.h"
#include "key.h"
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
const std::string kPageDetailCsvFileName = "page_bounds.csv";

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
      std::cerr << "Page full. Current size: " << page.GetNumRecords()
                << std::endl;
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
    const Manager::Options& options) {
  assert(options.use_segments);

  std::vector<SegmentFile> segment_files;
  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    segment_files.emplace_back(
        db_path / (kSegmentFilePrefix + std::to_string(i)),
        options.use_direct_io,
        /*pages_per_segment=*/SegmentBuilder::kSegmentPageCounts[i]);
  }
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  PageBuffer buf = PageMemoryAllocator::Allocate(
      /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back());

  // 1. Generate the segments.
  SegmentBuilder builder(options.records_per_page_goal,
                         options.records_per_page_delta);
  const auto segments = builder.Build(records);
  std::optional<std::ofstream> page_details;
  if (options.write_debug_info) {
    std::ofstream segment_details(db_path / kSegmentDetailCsvFileName);
    PrintSegmentsAsCSV(segment_details, segments);

    page_details = std::ofstream(db_path / kPageDetailCsvFileName);
    (*page_details) << "num_records,min_key,max_key" << std::endl;
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
            PageForKey(base_key, seg.model->line(), seg.page_count, rec.first);
        if (assigned_page != curr_page) {
          // Flush to page.
          const auto result = LoadIntoPage(
              buf, curr_page,
              /*lower_key=*/records[curr_page_first_record_idx].first,
              /*upper_key=*/rec.first, records,
              /*start_idx=*/curr_page_first_record_idx, /*end_idx=*/i);
          assert(result.ok());
          if (page_details.has_value()) {
            (*page_details) << (i - curr_page_first_record_idx) << ","
                            << records[curr_page_first_record_idx].first << ","
                            << rec.first << std::endl;
          }
          curr_page = assigned_page;
          curr_page_first_record_idx = i;
        }
      }
      const Key upper_key =
          (seg_idx == segments.size() - 1)
              // Max key if this is the last segment.
              ? std::numeric_limits<uint64_t>::max()
              // Otherwise, the next segment's first key.
              : records[segments[seg_idx + 1].start_idx].first;
      // Flush remaining to a page.
      const auto result =
          LoadIntoPage(buf, curr_page,
                       /*lower=*/records[curr_page_first_record_idx].first,
                       /*upper=*/upper_key, records, curr_page_first_record_idx,
                       seg.end_idx);
      assert(result.ok());
      if (page_details.has_value()) {
        (*page_details) << (seg.end_idx - curr_page_first_record_idx) << ","
                        << records[curr_page_first_record_idx].first << ","
                        << upper_key << std::endl;
      }

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
    const Manager::Options& options) {
  PageBuffer buf = PageMemoryAllocator::Allocate(/*num_pages=*/1);

  // One single file containing 4 KiB pages.
  std::vector<SegmentFile> segment_files;
  segment_files.emplace_back(db / (kSegmentFilePrefix + "0"),
                             options.use_direct_io, /*pages_per_segment=*/1);
  SegmentFile& sf = segment_files.front();

  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  size_t page_start_idx = 0;
  size_t page_end_idx = options.records_per_page_goal;
  while (page_end_idx <= records.size()) {
    memset(buf.get(), 0, pg::Page::kSize);
    auto result = LoadIntoPage(buf, 0, records[page_start_idx].first,
                               records[page_end_idx].first, records,
                               page_start_idx, page_end_idx);
    assert(result.ok());

    // Write page to disk.
    const size_t byte_offset = sf.AllocateSegment();
    sf.WritePages(byte_offset, buf.get(), /*num_pages=*/1);

    // Record the page boundary.
    SegmentId seg_id(/*file_id=*/0,
                     /*page_offset=*/byte_offset / pg::Page::kSize);
    segment_boundaries.emplace_back(
        records[page_start_idx].first,
        SegmentInfo(seg_id, std::optional<plr::Line64>()));

    page_start_idx = page_end_idx;
    page_end_idx = page_start_idx + options.records_per_page_goal;
  }
  if (page_start_idx < records.size()) {
    // Records that go on the last page.
    memset(buf.get(), 0, pg::Page::kSize);
    auto result = LoadIntoPage(buf, 0, records[page_start_idx].first,
                               std::numeric_limits<uint64_t>::max(), records,
                               page_start_idx, records.size());
    assert(result.ok());

    // Write page to disk.
    const size_t byte_offset = sf.AllocateSegment();
    sf.WritePages(byte_offset, buf.get(), /*num_pages=*/1);

    // Record the page boundary.
    SegmentId seg_id(/*file_id=*/0,
                     /*page_offset=*/byte_offset / pg::Page::kSize);
    segment_boundaries.emplace_back(
        records[page_start_idx].first,
        SegmentInfo(seg_id, std::optional<plr::Line64>()));
  }

  return Manager(db, std::move(segment_boundaries), std::move(segment_files));
}

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<SegmentFile> segment_files)
    : db_path_(std::move(db_path)), segment_files_(std::move(segment_files)) {
  index_.bulk_load(boundaries.begin(), boundaries.end());
}

Manager Manager::LoadIntoNew(const fs::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const Options& options) {
  fs::create_directory(db);

  if (options.use_segments) {
    return BulkLoadIntoSegments(db, records, options);
  } else {
    return BulkLoadIntoPages(db, records, options);
  }
}

Manager Manager::Reopen(const fs::path& db, const Options& options) {
  // Figure out if there are segments in this DB.
  const bool uses_segments = fs::exists(db / (kSegmentFilePrefix + "1"));
  PageBuffer buf = PageMemoryAllocator::Allocate(/*num_pages=*/1);
  Page page(buf.get());

  std::vector<SegmentFile> segment_files;
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    if (i > 0 && !uses_segments) break;
    const size_t pages_per_segment = SegmentBuilder::kSegmentPageCounts[i];
    segment_files.emplace_back(db / (kSegmentFilePrefix + std::to_string(i)),
                               options.use_direct_io, pages_per_segment);
    SegmentFile& sf = segment_files.back();

    const size_t num_segments = sf.NumSegments();
    const size_t bytes_per_segment = pages_per_segment * Page::kSize;
    for (size_t seg_idx = 0; seg_idx < num_segments; ++seg_idx) {
      sf.ReadPages(seg_idx * bytes_per_segment, buf.get(), /*num_pages=*/1);
      if (!page.IsValid() || page.IsOverflow()) {
        continue;
      }
      SegmentId id(i, seg_idx * pages_per_segment);  // Offset is the page offset.
      const Key base_key = key_utils::ExtractHead64(page.GetLowerBoundary());
      if (pages_per_segment == 1) {
        segment_boundaries.emplace_back(
            base_key, SegmentInfo(id, std::optional<plr::Line64>()));
      } else {
        segment_boundaries.emplace_back(base_key,
                                        SegmentInfo(id, page.GetModel()));
      }
    }
  }

  std::sort(segment_boundaries.begin(), segment_boundaries.end(),
            [](const std::pair<Key, SegmentInfo>& left,
               const std::pair<Key, SegmentInfo>& right) {
              return left.first < right.first;
            });

  return Manager(db, std::move(segment_boundaries), std::move(segment_files));
}

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
