#include <filesystem>
#include <fstream>
#include <vector>

#include "manager.h"
#include "persist/segment_wrap.h"
#include "util/key.h"

namespace fs = std::filesystem;

namespace {

using namespace llsm;
using namespace llsm::pg;

const std::string kSegmentSummaryCsvFileName = "segment_summary.csv";
const std::string kDebugDirName = "debug";

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

// Unused, but kept in case it is useful for debugging later on.
void PrintSegmentsAsCSV(std::ostream& out,
                        const std::vector<DatasetSegment>& segments) {
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

void PrintSegmentSummaryAsCsv(std::ostream& out,
                              const std::vector<DatasetSegment>& segments) {
  std::vector<size_t> num_segments;
  num_segments.resize(SegmentBuilder::kSegmentPageCounts.size());
  for (const auto& seg : segments) {
    const auto it = SegmentBuilder::kPageCountToSegment.find(seg.page_count);
    assert(it != SegmentBuilder::kPageCountToSegment.end());
    ++num_segments[it->second];
  }

  out << "segment_page_count,num_segments" << std::endl;
  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    out << (1ULL << i) << "," << num_segments[i] << std::endl;
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
  const auto segments = builder.BuildFromDataset(records);
  if (options.write_debug_info) {
    const auto debug_path = db_path / kDebugDirName;
    fs::create_directories(debug_path);
    std::ofstream segment_summary(debug_path / kSegmentSummaryCsvFileName);
    PrintSegmentSummaryAsCsv(segment_summary, segments);
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

    // 2. Set the checksum and sequence number.
    SegmentWrap sw(buf.get(), seg.page_count);
    sw.SetSequenceNumber(0);
    sw.ComputeAndSetChecksum();
    sw.ClearAllOverflows();

    // 3. Write the segment to disk.
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
                 std::move(segment_files), options, /*next_sequence_number=*/1,
                 FreeList());
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

    SegmentWrap sw(buf.get(), 1);
    sw.SetSequenceNumber(0);
    sw.ClearAllOverflows();

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

    SegmentWrap sw(buf.get(), 1);
    sw.SetSequenceNumber(0);
    sw.ClearAllOverflows();

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

  return Manager(db, std::move(segment_boundaries), std::move(segment_files),
                 options, /*next_sequence_number=*/1, FreeList());
}

}  // namespace pg
}  // namespace llsm
