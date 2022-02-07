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

Status LoadIntoPage(const PageBuffer& buf, size_t page_idx, Key lower,
                    Key upper, std::vector<Record>::const_iterator rec_begin,
                    std::vector<Record>::const_iterator rec_end) {
  key_utils::IntKeyAsSlice lower_key(lower), upper_key(upper);
  pg::Page page(buf.get() + pg::Page::kSize * page_idx, lower_key.as<Slice>(),
                upper_key.as<Slice>());
  for (auto it = rec_begin; it != rec_end; ++it) {
    key_utils::IntKeyAsSlice key(it->first);
    const auto res = page.Put(key.as<Slice>(), it->second);
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
                        const std::vector<Segment>& segments) {
  out << "segment_page_count,num_records,model_slope,model_intercept"
      << std::endl;
  for (const auto& seg : segments) {
    out << seg.page_count << "," << (seg.records.size()) << ",";
    if (seg.model.has_value()) {
      out << seg.model->line().slope() << "," << seg.model->line().intercept()
          << std::endl;
    } else {
      out << "," << std::endl;
    }
  }
}

void PrintSegmentSummaryAsCsv(std::ostream& out,
                              const std::vector<Segment>& segments) {
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

  // Open the segment files before constructing the `Manager`.
  std::vector<SegmentFile> segment_files;
  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    segment_files.emplace_back(
        db_path / (kSegmentFilePrefix + std::to_string(i)),
        /*pages_per_segment=*/SegmentBuilder::kSegmentPageCounts[i],
        options.use_buffered_io);
  }

  Manager m(db_path, {}, std::move(segment_files), options,
            /*next_sequence_number=*/0, FreeList());
  m.BulkLoadIntoSegmentsImpl(records);
  return m;
}

void Manager::BulkLoadIntoSegmentsImpl(const std::vector<Record>& records) {
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  // 1. Generate the segments.
  SegmentBuilder builder(options_.records_per_page_goal,
                         options_.records_per_page_delta);
  const auto segments = builder.BuildFromDataset(records);
  if (options_.write_debug_info) {
    const auto debug_path = db_path_ / kDebugDirName;
    fs::create_directories(debug_path);
    std::ofstream segment_summary(debug_path / kSegmentSummaryCsvFileName);
    PrintSegmentSummaryAsCsv(segment_summary, segments);
  }

  // 2. Load the data into pages on disk.
  const PageBuffer& buf = w_.buffer();
  for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
    const auto& seg = segments[seg_idx];
    const Key upper_bound = seg_idx == segments.size() - 1
                                ? std::numeric_limits<Key>::max()
                                : segments[seg_idx + 1].records.front().first;
    segment_boundaries.emplace_back(
        LoadIntoNewSegment(/*sequence_number=*/0, seg, upper_bound));
  }

  // Bulk load the index.
  index_.bulk_load(segment_boundaries.begin(), segment_boundaries.end());
}

Manager Manager::BulkLoadIntoPages(
    const fs::path& db, const std::vector<std::pair<Key, Slice>>& records,
    const Manager::Options& options) {
  // One single file containing 4 KiB pages.
  std::vector<SegmentFile> segment_files;
  segment_files.emplace_back(db / (kSegmentFilePrefix + "0"),
                             /*pages_per_segment=*/1, options.use_buffered_io);

  Manager m(db, {}, std::move(segment_files), options,
            /*next_sequence_number=*/0, FreeList());
  m.BulkLoadIntoPagesImpl(records);
  return m;
}

void Manager::BulkLoadIntoPagesImpl(const std::vector<Record>& records) {
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries =
      LoadIntoNewPages(/*sequence_number=*/0, records.front().first,
                       std::numeric_limits<Key>::max(), records.begin(),
                       records.end());
  index_.bulk_load(segment_boundaries.begin(), segment_boundaries.end());
}

std::pair<Key, SegmentInfo> Manager::LoadIntoNewSegment(
    const uint32_t sequence_number, const Segment& seg, const Key upper_bound) {
  assert(!seg.records.empty());

  const Key base_key = seg.records[0].first;
  const PageBuffer& buf = w_.buffer();
  memset(buf.get(), 0, pg::Page::kSize * seg.page_count);
  if (seg.page_count > 1) {
    // Partition the records into pages based on the model.
    size_t curr_page = 0;
    size_t curr_page_first_record_idx = 0;
    for (size_t i = 0; i < seg.records.size(); ++i) {
      const auto& rec = seg.records[i];
      // Use the page assigned by the model.
      const size_t assigned_page =
          PageForKey(base_key, seg.model->line(), seg.page_count, rec.first);
      if (assigned_page != curr_page) {
        // Flush to page.
        const auto result = LoadIntoPage(
            buf, curr_page,
            /*lower_key=*/seg.records[curr_page_first_record_idx].first,
            /*upper_key=*/rec.first,
            seg.records.begin() + curr_page_first_record_idx,
            seg.records.begin() + i);
        assert(result.ok());
        curr_page = assigned_page;
        curr_page_first_record_idx = i;
      }
    }
    // Flush remaining to a page.
    const auto result = LoadIntoPage(
        buf, curr_page,
        /*lower=*/seg.records[curr_page_first_record_idx].first,
        /*upper=*/upper_bound, seg.records.begin() + curr_page_first_record_idx,
        seg.records.end());
    assert(result.ok());

    // Write model into the first page (for deserialization).
    pg::Page first_page(buf.get());
    first_page.SetModel(seg.model->line());

  } else {
    // Simple case - put all the records into one page.
    assert(seg.page_count == 1);
    const auto result =
        LoadIntoPage(buf, 0, /*lower=*/seg.records.front().first, upper_bound,
                     seg.records.begin(), seg.records.end());
    assert(result.ok());
  }

  // 2. Set the checksum and sequence number.
  SegmentWrap sw(buf.get(), seg.page_count);
  sw.SetSequenceNumber(sequence_number);
  sw.ComputeAndSetChecksum();
  sw.ClearAllOverflows();

  // 3. Write the segment to disk.
  const size_t segment_idx =
      SegmentBuilder::kPageCountToSegment.find(seg.page_count)->second;
  SegmentFile& sf = segment_files_[segment_idx];

  // Either use an existing free segment or allocate a new one.
  SegmentId seg_id;
  const auto maybe_seg_id = free_.Get(seg.page_count);
  if (maybe_seg_id.has_value()) {
    seg_id = *maybe_seg_id;
  } else {
    const size_t byte_offset = sf.AllocateSegment();
    seg_id = SegmentId(/*file_offset=*/segment_idx,
                       /*page_offset=*/byte_offset / pg::Page::kSize);
  }

  sf.WritePages(seg_id.GetOffset() * Page::kSize, buf.get(), seg.page_count);
  w_.BumpWriteCount(seg.page_count);
  return std::make_pair(
      base_key, SegmentInfo(seg_id, seg.model.has_value()
                                        ? seg.model->line()
                                        : std::optional<plr::Line64>()));
}

std::vector<std::pair<Key, SegmentInfo>> Manager::LoadIntoNewPages(
    const uint32_t sequence_number, const Key lower_bound,
    const Key upper_bound, const std::vector<Record>::const_iterator rec_begin,
    const std::vector<Record>::const_iterator rec_end) {
  SegmentFile& sf = segment_files_.front();
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;

  size_t page_start_idx = 0;
  size_t page_end_idx = options_.records_per_page_goal;
  const size_t num_records = rec_end - rec_begin;

  const PageBuffer& buf = w_.buffer();
  while (page_end_idx <= num_records) {
    memset(buf.get(), 0, pg::Page::kSize);
    const auto page_begin = rec_begin + page_start_idx;
    const auto page_end = rec_begin + page_end_idx;
    const Key lower = page_start_idx == 0 ? lower_bound : page_begin->first;
    const Key upper =
        page_end_idx == num_records ? upper_bound : page_end->first;
    auto result = LoadIntoPage(buf, 0, lower, upper, page_begin, page_end);
    assert(result.ok());

    SegmentWrap sw(buf.get(), 1);
    sw.SetSequenceNumber(sequence_number);
    sw.ClearAllOverflows();

    // Write page to disk.
    SegmentId seg_id;
    const auto maybe_seg_id = free_.Get(/*page_count=*/1);
    if (maybe_seg_id.has_value()) {
      seg_id = *maybe_seg_id;
    } else {
      const size_t byte_offset = sf.AllocateSegment();
      seg_id = SegmentId(/*file_id=*/0,
                         /*page_offset=*/byte_offset / pg::Page::kSize);
    }
    sf.WritePages(seg_id.GetOffset() * Page::kSize, buf.get(), /*num_pages=*/1);
    w_.BumpWriteCount(1);

    // Record the page boundary.
    segment_boundaries.emplace_back(
        lower, SegmentInfo(seg_id, std::optional<plr::Line64>()));

    page_start_idx = page_end_idx;
    page_end_idx = page_start_idx + options_.records_per_page_goal;
  }
  if (page_start_idx < num_records) {
    // Records that go on the last page.
    memset(buf.get(), 0, pg::Page::kSize);
    const auto page_begin = rec_begin + page_start_idx;
    const Key lower = page_start_idx == 0 ? lower_bound : page_begin->first;
    auto result = LoadIntoPage(buf, 0, lower, upper_bound, page_begin, rec_end);
    assert(result.ok());

    SegmentWrap sw(buf.get(), 1);
    sw.SetSequenceNumber(0);
    sw.ClearAllOverflows();

    // Write page to disk.
    SegmentId seg_id;
    const auto maybe_seg_id = free_.Get(/*page_count=*/1);
    if (maybe_seg_id.has_value()) {
      seg_id = *maybe_seg_id;
    } else {
      const size_t byte_offset = sf.AllocateSegment();
      seg_id = SegmentId(/*file_id=*/0,
                         /*page_offset=*/byte_offset / pg::Page::kSize);
    }
    sf.WritePages(seg_id.GetOffset() * Page::kSize, buf.get(), /*num_pages=*/1);
    w_.BumpWriteCount(1);

    segment_boundaries.emplace_back(
        lower, SegmentInfo(seg_id, std::optional<plr::Line64>()));
  }

  return segment_boundaries;
}

}  // namespace pg
}  // namespace llsm
