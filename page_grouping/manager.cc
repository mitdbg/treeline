#include "manager.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "bufmgr/page_memory_allocator.h"
#include "key.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "persist/segment_wrap.h"
#include "segment_builder.h"
#include "util/key.h"

using namespace llsm;
using namespace llsm::pg;
namespace fs = std::filesystem;

namespace {

const std::string kSegmentFilePrefix = "sf-";
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

thread_local Workspace Manager::w_;

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

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<SegmentFile> segment_files, Options options,
                 uint32_t next_sequence_number, FreeList free)
    : db_path_(std::move(db_path)),
      segment_files_(std::move(segment_files)),
      next_sequence_number_(next_sequence_number),
      free_(std::move(free)),
      options_(std::move(options)) {
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
  PageBuffer buf = PageMemoryAllocator::Allocate(/*num_pages=*/2);
  Page page(buf.get());

  std::vector<SegmentFile> segment_files;
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;
  FreeList free;
  uint32_t max_sequence = 0;

  for (size_t i = 0; i < SegmentBuilder::kSegmentPageCounts.size(); ++i) {
    if (i > 0 && !uses_segments) break;
    const size_t pages_per_segment = SegmentBuilder::kSegmentPageCounts[i];
    segment_files.emplace_back(db / (kSegmentFilePrefix + std::to_string(i)),
                               options.use_direct_io, pages_per_segment);
    SegmentFile& sf = segment_files.back();

    const size_t num_segments = sf.NumAllocatedSegments();
    const size_t bytes_per_segment = pages_per_segment * Page::kSize;
    for (size_t seg_idx = 0; seg_idx < num_segments; ++seg_idx) {
      // Offset in `id` is the page offset.
      SegmentId id(i, seg_idx * pages_per_segment);
      // TODO: We should also validate the checksum (part of recovery).
      if (pages_per_segment == 1) {
        sf.ReadPages(seg_idx * bytes_per_segment, buf.get(), /*num_pages=*/1);
      } else {
        sf.ReadPages(seg_idx * bytes_per_segment, buf.get(), /*num_pages=*/2);
      }
      if (!page.IsValid()) {
        // This segment is a "hole" in the file and can be reused.
        free.Add(id);
        continue;
      }
      if (page.IsOverflow()) {
        continue;
      }

      const Key base_key = key_utils::ExtractHead64(page.GetLowerBoundary());
      if (pages_per_segment == 1) {
        segment_boundaries.emplace_back(
            base_key, SegmentInfo(id, std::optional<plr::Line64>()));
      } else {
        segment_boundaries.emplace_back(base_key,
                                        SegmentInfo(id, page.GetModel()));
      }

      // Extract the sequence number.
      SegmentWrap sw(buf.get(), pages_per_segment);
      max_sequence = std::max(max_sequence, sw.GetSequenceNumber());
    }
  }

  std::sort(segment_boundaries.begin(), segment_boundaries.end(),
            [](const std::pair<Key, SegmentInfo>& left,
               const std::pair<Key, SegmentInfo>& right) {
              return left.first < right.first;
            });

  return Manager(db, std::move(segment_boundaries), std::move(segment_files),
                 options, /*next_segment_index=*/max_sequence + 1,
                 std::move(free));
}

Status Manager::Get(const Key& key, std::string* value_out) {
  // 1. Find the segment that should hold the key.
  auto it = index_.upper_bound(key);
  if (it != index_.begin()) {
    --it;
  }

  // 2. Figure out the page offset.
  const Key base_key = it->first;
  const size_t page_idx = it->second.PageForKey(base_key, key);

  // 3. Read the page in (there are no overflows right now).
  const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
  const size_t page_offset = it->second.id().GetOffset() + page_idx;
  sf.ReadPages(page_offset * pg::Page::kSize, w_.buffer().get(),
               /*num_pages=*/1);
  w_.BumpReadCount(1);

  // 4. Search for the record on the page.
  // TODO: Lock the page.
  pg::Page page(w_.buffer().get());
  key_utils::IntKeyAsSlice key_slice(key);
  auto status = page.Get(key_slice.as<Slice>(), value_out);
  if (status.ok()) {
    return status;
  }

  // 5. Check the overflow page if it exists.
  // TODO: We always assume at most 1 overflow page.
  if (!page.HasOverflow()) {
    return Status::NotFound("Record does not exist.");
  }
  const SegmentId overflow_id = page.GetOverflow();
  // All overflow pages are single pages.
  assert(overflow_id.GetFileId() == 0);
  const SegmentFile& osf = segment_files_[overflow_id.GetFileId()];
  osf.ReadPages(overflow_id.GetOffset() * pg::Page::kSize, w_.buffer().get(),
                /*num_pages=*/1);
  w_.BumpReadCount(1);
  return page.Get(key_slice.as<Slice>(), value_out);
}

Status Manager::PutBatch(const std::vector<std::pair<Key, Slice>>& records) {
  if (records.empty()) return Status::OK();
  // TODO: Support deletes.

  const auto start_it = SegmentForKey(records.front().first);
  size_t start_idx = 0;
  Key curr_base = start_it->first;
  SegmentInfo curr_sinfo = start_it->second;

  for (size_t i = 1; i < records.size(); ++i) {
    // TODO: We query the index for each key in the batch. This may be
    // expensive, but this is also the approach we took in the original LLSM
    // implementation. We can make this batching strategy fancier if this is a
    // bottleneck.
    const auto it = SegmentForKey(records[i].first);
    if (it->first != curr_base) {
      // The records in [start_idx, i) belong to the same segment.
      // Write out segment
      const auto status =
          WriteToSegment(curr_base, curr_sinfo, records, start_idx, i);
      if (!status.ok()) {
        return status;
      }
      curr_base = it->first;
      curr_sinfo = it->second;
      start_idx = i;
    }
  }

  WriteToSegment(curr_base, curr_sinfo, records, start_idx, records.size());
  return Status::OK();
}

Status Manager::WriteToSegment(
    Key segment_base, const SegmentInfo& sinfo,
    const std::vector<std::pair<Key, Slice>>& records, size_t start_idx,
    size_t end_idx) {
  // TODO: Need to lock the segment and pages.

  // Simplifying assumptions:
  // - If the number of writes is past a certain threshold
  //   (`record_per_page_goal` x `pages_in_segment` x 2), we don't attempt to
  //   make the writes. We immediately trigger a segment reorg.
  // - Records are the same size.
  // - No deletes. So it's always safe to call `Put()` on the page.
  // - As soon as we try to insert into a full overflow page, we trigger a
  //   segment reorg.

  const size_t reorg_threshold =
      options_.records_per_page_goal * sinfo.page_count() * 2ULL;
  if (end_idx - start_idx > reorg_threshold) {
    // TODO: Immediately trigger reorg.
    return Status::NotSupported("Requires segment reorg.");
  }

  void* orig_page_buf = w_.buffer().get();
  void* overflow_page_buf = w_.buffer().get() + pg::Page::kSize;
  pg::Page orig_page(orig_page_buf);
  pg::Page overflow_page(overflow_page_buf);

  size_t curr_page_idx =
      sinfo.PageForKey(segment_base, records[start_idx].first);
  ReadPage(sinfo.id(), curr_page_idx, orig_page_buf);

  SegmentId overflow_page_id;
  bool orig_page_dirty = false;
  bool overflow_page_dirty = false;

  pg::Page* curr_page = &orig_page;
  bool* curr_page_dirty = &orig_page_dirty;

  auto write_dirty_pages = [&]() {
    // Write out overflow first to avoid dangling overflow pointers.
    if (overflow_page_dirty) {
      WritePage(overflow_page_id, 0, overflow_page_buf);
    }
    if (curr_page_dirty) {
      WritePage(sinfo.id(), curr_page_idx, orig_page_buf);
    }
  };
  auto write_record_to_chain = [&](Key key, const Slice& value) {
    key_utils::IntKeyAsSlice key_slice(key);
    auto status = curr_page->Put(key_slice.as<Slice>(), value);
    if (status.ok()) {
      *curr_page_dirty = true;
      return true;
    }

    // `curr_page` is full. Create/load an overflow page if possible.
    if (curr_page == &overflow_page) {
      // Cannot allocate another overflow (reached max length)
      return false;
    }

    // Load the overflow page.
    if (curr_page->HasOverflow()) {
      overflow_page_id = curr_page->GetOverflow();
      ReadPage(overflow_page_id, 0, overflow_page_buf);
      curr_page = &overflow_page;
      curr_page_dirty = &overflow_page_dirty;

    } else {
      // Allocate a new page.
      const auto maybe_free_page = free_.Get(/*page_count=*/1);
      if (maybe_free_page.has_value()) {
        overflow_page_id = *maybe_free_page;
      } else {
        // Allocate a new free page.
        const size_t byte_offset = segment_files_[0].AllocateSegment();
        overflow_page_id = SegmentId(0, byte_offset / pg::Page::kSize);
      }

      memset(overflow_page_buf, 0, pg::Page::kSize);
      overflow_page.MakeOverflow();
      overflow_page_dirty = true;
      curr_page = &overflow_page;
      curr_page_dirty = &overflow_page_dirty;
    }

    // Attempt to write to the overflow page.
    status = curr_page->Put(key_slice.as<Slice>(), value);
    if (status.ok()) {
      *curr_page_dirty = true;
      return true;
    } else {
      // Overflow is full too.
      return false;
    }
  };

  for (size_t i = start_idx; i < end_idx; ++i) {
    const size_t page_idx = sinfo.PageForKey(segment_base, records[i].first);
    if (page_idx != curr_page_idx) {
      write_dirty_pages();
      // Update the current page.
      curr_page_idx = page_idx;
      ReadPage(sinfo.id(), curr_page_idx, orig_page_buf);
      overflow_page_id = SegmentId();
      orig_page_dirty = false;
      overflow_page_dirty = false;
      curr_page = &orig_page;
      curr_page_dirty = &orig_page_dirty;
    }

    const bool succeeded =
        write_record_to_chain(records[i].first, records[i].second);
    if (!succeeded) {
      write_dirty_pages();
      // TODO: Trigger segment reorganization here (also include the records
      //       in range [i, end_idx)).
      return Status::NotSupported("Requires segment reorg.");
    }
  }

  write_dirty_pages();

  return Status::OK();
}

Status Manager::ScanWithEstimates(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  // Scan strategy (all scans are forward scans):
  // - Find segment containing starting key.
  // - Estimate how much of the segment to read based on the position of the
  // key.
  // - Scan forward, reading the whole segment when able.
  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  auto it = index_.upper_bound(start_key);
  if (it != index_.begin()) {
    --it;
  }

  // 2. Estimate how much of the segment to read based on the position of the
  // key.
  size_t est_start_pages_to_read = 1;
  size_t start_page_idx = 0;
  const size_t first_segment_page_count = it->second.page_count();
  if (first_segment_page_count > 1) {
    const Key base_key = it->first;
    const double pos =
        std::max(0.0, it->second.model()->operator()(start_key - base_key));
    const size_t page_idx = static_cast<size_t>(pos);
    if (page_idx >= first_segment_page_count) {
      // Edge case due to numeric errors.
      start_page_idx = first_segment_page_count - 1;
      est_start_pages_to_read = 1;
    } else {
      // Use the predicted position to estimate how many more pages we need to
      // read.
      const double page_pos = pos - page_idx;
      const int64_t est_matching_records_on_first_page =
          options_.records_per_page_goal -
          (page_pos * options_.records_per_page_goal);
      const int64_t est_remaining_records =
          records_left - est_matching_records_on_first_page;

      start_page_idx = page_idx;
      if (est_remaining_records > 0) {
        const size_t est_remaining_pages =
            std::ceil(est_remaining_records /
                      static_cast<double>(options_.records_per_page_goal));
        est_start_pages_to_read =
            1 + std::min(est_remaining_pages,
                         first_segment_page_count - page_idx - 1);
      } else {
        est_start_pages_to_read = 1;
      }
    }
  }

  // 3. Start scanning the first segment, reading in more pages as needed if
  // our estimate was incorrect.
  const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
  const size_t segment_byte_offset = it->second.id().GetOffset() * Page::kSize;
  sf.ReadPages(segment_byte_offset + start_page_idx * Page::kSize,
               w_.buffer().get(), est_start_pages_to_read);
  w_.BumpReadCount(est_start_pages_to_read);

  // Scan the first page.
  Page first_page(w_.buffer().get());
  auto page_it = first_page.GetIterator();
  key_utils::IntKeyAsSlice start_key_slice(start_key);
  page_it.Seek(start_key_slice.as<Slice>());
  for (; records_left > 0 && page_it.Valid(); --records_left, page_it.Next()) {
    values_out->emplace_back(key_utils::ExtractHead64(page_it.key()),
                             page_it.value().ToString());
  }

  // Common code used to scan a whole page.
  const auto scan_page = [&records_left, values_out](const Page& page) {
    for (auto page_it = page.GetIterator(); records_left > 0 && page_it.Valid();
         --records_left, page_it.Next()) {
      values_out->emplace_back(key_utils::ExtractHead64(page_it.key()),
                               page_it.value().ToString());
    }
  };

  // Scan the rest of the pages in the segment that we read in.
  size_t start_seg_page_idx = start_page_idx + 1;
  while (records_left > 0 &&
         start_seg_page_idx < (start_page_idx + est_start_pages_to_read)) {
    Page page(w_.buffer().get() +
              (start_seg_page_idx - start_page_idx) * Page::kSize);
    scan_page(page);
    ++start_seg_page_idx;
  }

  // If we estimated incorrectly and we still have more pages to read in the
  // first segment.
  while (records_left > 0 && start_seg_page_idx < first_segment_page_count) {
    // Read 1 page at a time.
    sf.ReadPages(segment_byte_offset + start_seg_page_idx * Page::kSize,
                 w_.buffer().get(), /*num_pages=*/1);
    w_.BumpReadCount(1);
    Page page(w_.buffer().get());
    scan_page(page);
    ++start_seg_page_idx;
  }

  // 4. Done reading the first segment. Now keep scanning forward as far as
  // needed.
  if (it != index_.end()) {
    ++it;
  }
  while (records_left > 0 && it != index_.end()) {
    const size_t seg_page_count = it->second.page_count();
    const size_t seg_byte_offset = it->second.id().GetOffset() * Page::kSize;
    const size_t est_pages_left = std::ceil(
        records_left / static_cast<double>(options_.records_per_page_goal));

    // Estimate the number of pages to read from the segment. The idea is to
    // avoid reading the whole segment if we do not anticipate needing to scan
    // all the records in the segment.
    const size_t pages_to_read = std::min(seg_page_count, est_pages_left);
    const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
    sf.ReadPages(seg_byte_offset, w_.buffer().get(), pages_to_read);
    w_.BumpReadCount(pages_to_read);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < pages_to_read) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      ++page_idx;
    }

    // If we underestimated and need to read a few more pages from this
    // segment.
    while (records_left > 0 && page_idx < seg_page_count) {
      // Read 1 page at a time.
      sf.ReadPages(seg_byte_offset + page_idx * Page::kSize, w_.buffer().get(),
                   /*num_pages=*/1);
      w_.BumpReadCount(1);
      Page page(w_.buffer().get());
      scan_page(page);
      ++page_idx;
    }

    // Go to the next segment.
    ++it;
  }

  return Status::OK();
}

Status Manager::ScanWhole(
    const Key& start_key, const size_t amount,
    std::vector<std::pair<Key, std::string>>* values_out) {
  values_out->clear();
  values_out->reserve(amount);

  if (amount == 0) return Status::OK();
  size_t records_left = amount;

  // 1. Find the segment that should hold the start key.
  auto it = index_.upper_bound(start_key);
  if (it != index_.begin()) {
    --it;
  }

  // 2. Read the first segment.
  const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
  const size_t first_segment_size = it->second.page_count();
  const size_t segment_byte_offset = it->second.id().GetOffset() * Page::kSize;
  sf.ReadPages(segment_byte_offset, w_.buffer().get(), first_segment_size);
  w_.BumpReadCount(first_segment_size);

  // 3. Scan the first matching page in the segment.
  size_t start_segment_page_idx = it->second.PageForKey(it->first, start_key);
  Page first_page(w_.buffer().get() + start_segment_page_idx * Page::kSize);
  auto page_it = first_page.GetIterator();
  key_utils::IntKeyAsSlice start_key_slice(start_key);
  page_it.Seek(start_key_slice.as<Slice>());
  for (; page_it.Valid() && records_left > 0; page_it.Next(), --records_left) {
    values_out->emplace_back(key_utils::ExtractHead64(page_it.key()),
                             page_it.value().ToString());
  }

  // Common code used to scan a whole page.
  const auto scan_page = [&records_left, values_out](const Page& page) {
    for (auto page_it = page.GetIterator(); records_left > 0 && page_it.Valid();
         --records_left, page_it.Next()) {
      values_out->emplace_back(key_utils::ExtractHead64(page_it.key()),
                               page_it.value().ToString());
    }
  };

  // 4. Scan the rest of the pages in the segment.
  ++start_segment_page_idx;
  while (records_left > 0 && start_segment_page_idx < first_segment_size) {
    Page page(w_.buffer().get() + start_segment_page_idx * Page::kSize);
    scan_page(page);
    ++start_segment_page_idx;
  }

  // 5. Scan forward until we read enough records or run out of segments to
  //    read.
  if (records_left > 0 && it != index_.end()) {
    ++it;
  }
  while (records_left > 0 && it != index_.end()) {
    const size_t seg_page_count = it->second.page_count();
    const size_t seg_byte_offset = it->second.id().GetOffset() * Page::kSize;

    const SegmentFile& sf = segment_files_[it->second.id().GetFileId()];
    sf.ReadPages(seg_byte_offset, w_.buffer().get(), seg_page_count);
    w_.BumpReadCount(seg_page_count);

    size_t page_idx = 0;
    while (records_left > 0 && page_idx < seg_page_count) {
      Page page(w_.buffer().get() + page_idx * Page::kSize);
      scan_page(page);
      ++page_idx;
    }

    // Go to the next segment.
    ++it;
  }

  return Status::OK();
}

void Manager::ReadPage(const SegmentId& seg_id, size_t page_idx, void* buffer) {
  assert(seg_id.IsValid());
  const SegmentFile& sf = segment_files_[seg_id.GetFileId()];
  sf.ReadPages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
               /*num_pages=*/1);
  w_.BumpReadCount(1);
}

void Manager::WritePage(const SegmentId& seg_id, size_t page_idx,
                        void* buffer) {
  assert(seg_id.IsValid());
  const SegmentFile& sf = segment_files_[seg_id.GetFileId()];
  sf.WritePages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
                /*num_pages=*/1);
}

}  // namespace pg
}  // namespace llsm
