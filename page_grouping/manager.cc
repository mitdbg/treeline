#include "manager.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "bufmgr/page_memory_allocator.h"
#include "key.h"
#include "persist/merge_iterator.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "persist/segment_wrap.h"
#include "segment_builder.h"
#include "util/key.h"

namespace fs = std::filesystem;

namespace llsm {
namespace pg {

thread_local Workspace Manager::w_;
const std::string Manager::kSegmentFilePrefix = "sf-";

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<SegmentFile> segment_files, Options options,
                 uint32_t next_sequence_number, FreeList free)
    : db_path_(std::move(db_path)),
      segment_files_(std::move(segment_files)),
      next_sequence_number_(next_sequence_number),
      free_(std::move(free)),
      options_(std::move(options)) {
  if (!boundaries.empty()) {
    index_.bulk_load(boundaries.begin(), boundaries.end());
  }
  if (options_.num_bg_threads > 0) {
    bg_threads_ = std::make_unique<ThreadPool>(options_.num_bg_threads);
  }
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
  PageBuffer buf = PageMemoryAllocator::Allocate(
      /*num_pages=*/SegmentBuilder::kSegmentPageCounts.back());
  Page first_page(buf.get());

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
      sf.ReadPages(seg_idx * bytes_per_segment, buf.get(), pages_per_segment);

      SegmentWrap sw(buf.get(), pages_per_segment);
      if (!sw.CheckChecksum() || !first_page.IsValid()) {
        // This segment is a "hole" in the file and can be reused.
        free.Add(id);
        continue;
      }
      if (first_page.IsOverflow()) {
        continue;
      }

      const Key base_key =
          key_utils::ExtractHead64(first_page.GetLowerBoundary());
      if (pages_per_segment == 1) {
        segment_boundaries.emplace_back(
            base_key, SegmentInfo(id, std::optional<plr::Line64>()));
      } else {
        segment_boundaries.emplace_back(base_key,
                                        SegmentInfo(id, first_page.GetModel()));
      }

      // Extract the sequence number.
      max_sequence = std::max(max_sequence, sw.GetSequenceNumber());

      // Keep track of whether or not the segment has an overflow.
      segment_boundaries.back().second.SetOverflow(sw.HasOverflow());
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
  SegmentInfo* curr_sinfo = &(start_it->second);

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
      curr_sinfo = &(it->second);
      start_idx = i;
    }
  }

  WriteToSegment(curr_base, curr_sinfo, records, start_idx, records.size());
  return Status::OK();
}

Status Manager::WriteToSegment(
    Key segment_base, SegmentInfo* sinfo,
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
      options_.records_per_page_goal * sinfo->page_count() * 2ULL;
  if (end_idx - start_idx > reorg_threshold) {
    // TODO: Immediately trigger reorg.
    return Status::NotSupported("Requires segment reorg.");
  }

  void* orig_page_buf = w_.buffer().get();
  void* overflow_page_buf = w_.buffer().get() + pg::Page::kSize;
  pg::Page orig_page(orig_page_buf);
  pg::Page overflow_page(overflow_page_buf);

  size_t curr_page_idx =
      sinfo->PageForKey(segment_base, records[start_idx].first);
  ReadPage(sinfo->id(), curr_page_idx, orig_page_buf);

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
      WritePage(sinfo->id(), curr_page_idx, orig_page_buf);
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
      overflow_page = Page(overflow_page_buf, orig_page);
      overflow_page.MakeOverflow();
      overflow_page.SetOverflow(SegmentId());
      overflow_page_dirty = true;
      sinfo->SetOverflow(true);

      orig_page.SetOverflow(overflow_page_id);
      orig_page_dirty = true;

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
    const size_t page_idx = sinfo->PageForKey(segment_base, records[i].first);
    if (page_idx != curr_page_idx) {
      write_dirty_pages();
      // Update the current page.
      curr_page_idx = page_idx;
      ReadPage(sinfo->id(), curr_page_idx, orig_page_buf);
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

void Manager::ReadPage(const SegmentId& seg_id, size_t page_idx,
                       void* buffer) const {
  assert(seg_id.IsValid());
  const SegmentFile& sf = segment_files_[seg_id.GetFileId()];
  sf.ReadPages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
               /*num_pages=*/1);
  w_.BumpReadCount(1);
}

void Manager::WritePage(const SegmentId& seg_id, size_t page_idx,
                        void* buffer) const {
  assert(seg_id.IsValid());
  const SegmentFile& sf = segment_files_[seg_id.GetFileId()];
  sf.WritePages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
                /*num_pages=*/1);
}

void Manager::ReadSegment(const SegmentId& seg_id) const {
  assert(seg_id.IsValid());
  const SegmentFile& sf = segment_files_[seg_id.GetFileId()];
  sf.ReadPages(seg_id.GetOffset() * pg::Page::kSize, w_.buffer().get(),
               sf.PagesPerSegment());
  w_.BumpReadCount(sf.PagesPerSegment());
}

void Manager::ReadOverflows(
    const std::vector<std::pair<SegmentId, void*>>& overflows_to_read) const {
  if (bg_threads_ != nullptr) {
    std::vector<std::future<void>> futures;
    futures.reserve(overflows_to_read.size());
    for (const auto& otr : overflows_to_read) {
      futures.push_back(bg_threads_->Submit(
          [this, &otr]() { ReadPage(otr.first, 0, otr.second); }));
    }
    for (auto& f : futures) {
      f.get();
    }

  } else {
    for (const auto& otr : overflows_to_read) {
      ReadPage(otr.first, 0, otr.second);
    }
  }
}

}  // namespace pg
}  // namespace llsm
