#include "manager.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include "bufmgr/page_memory_allocator.h"
#include "key.h"
#include "llsm/pg_db.h"
#include "persist/merge_iterator.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "persist/segment_wrap.h"
#include "segment_builder.h"
#include "util/key.h"

namespace fs = std::filesystem;

namespace llsm {
namespace pg {

using SegmentMode = LockManager::SegmentMode;
using PageMode = LockManager::PageMode;

thread_local Workspace Manager::w_;
const std::string Manager::kSegmentFilePrefix = "sf-";

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<SegmentFile> segment_files,
                 PageGroupedDBOptions options, uint32_t next_sequence_number,
                 FreeList free)
    : db_path_(std::move(db_path)),
      lock_manager_(std::make_shared<LockManager>()),
      index_(std::make_unique<SegmentIndex>(lock_manager_)),
      segment_files_(std::move(segment_files)),
      next_sequence_number_(next_sequence_number),
      free_(std::move(free)),
      options_(std::move(options)) {
  if (!boundaries.empty()) {
    index_->BulkLoadFromEmpty(boundaries.begin(), boundaries.end());
  }
  if (options_.num_bg_threads > 0) {
    bg_threads_ = std::make_unique<ThreadPool>(options_.num_bg_threads);
  }
}

Manager Manager::LoadIntoNew(const fs::path& db,
                             const std::vector<std::pair<Key, Slice>>& records,
                             const PageGroupedDBOptions& options) {
  fs::create_directory(db);

  if (options.use_segments) {
    return BulkLoadIntoSegments(db, records, options);
  } else {
    return BulkLoadIntoPages(db, records, options);
  }
}

Manager Manager::Reopen(const fs::path& db,
                        const PageGroupedDBOptions& options) {
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
                               pages_per_segment, options.use_memory_based_io);
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
  return GetWithPages(key, value_out).first;
}

std::pair<Status, std::vector<pg::Page>> Manager::GetWithPages(
    const Key& key, std::string* value_out) {
  void* main_page_buf = w_.buffer().get();
  void* overflow_page_buf = w_.buffer().get() + pg::Page::kSize;

  // 1. Find the segment that should hold the key.
  const auto [base_key, sinfo] =
      index_->SegmentForKeyWithLock(key, SegmentMode::kPageRead);

  // 2. Figure out the page offset, lock the page, and then read it in.
  const size_t page_idx = sinfo.PageForKey(base_key, key);
  lock_manager_->AcquirePageLock(sinfo.id(), page_idx, PageMode::kShared);
  ReadPage(sinfo.id(), page_idx, main_page_buf);

  // 3. Search for the record on the page.
  pg::Page main_page(main_page_buf);
  key_utils::IntKeyAsSlice key_slice(key);
  auto status = main_page.Get(key_slice.as<Slice>(), value_out);
  if (status.ok()) {
    lock_manager_->ReleasePageLock(sinfo.id(), page_idx, PageMode::kShared);
    lock_manager_->ReleaseSegmentLock(sinfo.id(), SegmentMode::kPageRead);
    return {status, {main_page}};
  }

  // 4. Check the overflow page if it exists.
  // TODO: We always assume at most 1 overflow page.
  if (!main_page.HasOverflow()) {
    lock_manager_->ReleasePageLock(sinfo.id(), page_idx, PageMode::kShared);
    lock_manager_->ReleaseSegmentLock(sinfo.id(), SegmentMode::kPageRead);
    return {Status::NotFound("Record does not exist."), {main_page}};
  }
  const SegmentId overflow_id = main_page.GetOverflow();
  // All overflow pages are single pages.
  assert(overflow_id.GetFileId() == 0);
  ReadPage(overflow_id, /*page_idx=*/0, overflow_page_buf);
  pg::Page overflow_page(overflow_page_buf);
  status = overflow_page.Get(key_slice.as<Slice>(), value_out);

  lock_manager_->ReleasePageLock(sinfo.id(), page_idx, PageMode::kShared);
  lock_manager_->ReleaseSegmentLock(sinfo.id(), SegmentMode::kPageRead);
  return {status, {main_page, overflow_page}};
}

Status Manager::PutBatch(const std::vector<std::pair<Key, Slice>>& records) {
  if (records.empty()) return Status::OK();
  // TODO: Support deletes.

  SegmentIndex::Entry curr = index_->SegmentForKey(records.front().first);
  size_t start_idx = 0;

  for (size_t i = 1; i < records.size(); ++i) {
    // TODO: We query the index for each key in the batch. This may be
    // expensive, but this is also the approach we took in the original LLSM
    // implementation. We can make this batching strategy fancier if this is a
    // bottleneck.
    const auto [rec_base, _] = index_->SegmentForKey(records[i].first);
    if (rec_base != curr.first) {
      // The records in [start_idx, i) belong to the same segment.
      // Write out segment
      const auto status = WriteToSegment(curr, records, start_idx, i);
      if (!status.ok()) {
        return status;
      }

      // Important to redo the lookup since `WriteToSegment()` might have
      // modified the index.
      curr = index_->SegmentForKey(records[i].first);
      start_idx = i;
    }
  }

  WriteToSegment(curr, records, start_idx, records.size());
  return Status::OK();
}

Status Manager::WriteToSegment(
    const SegmentIndex::Entry& segment,
    const std::vector<std::pair<Key, Slice>>& records, size_t start_idx,
    size_t end_idx) {
  // TODO: Need to lock the segment and pages.
  const auto& [segment_base, sinfo] = segment;

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
    // Immediately trigger a segment rewrite that merges in the records.
    if (options_.use_segments) {
      RewriteSegments(segment_base, records.begin() + start_idx,
                      records.begin() + end_idx);
    } else {
      FlattenChain(segment_base, records.begin() + start_idx,
                   records.begin() + end_idx);
    }
    return Status::OK();
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

  auto write_dirty_pages = [&, segment_base = segment_base, sinfo = sinfo]() {
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
      overflow_page = Page(overflow_page_buf, orig_page);
      overflow_page.MakeOverflow();
      overflow_page.SetOverflow(SegmentId());
      overflow_page_dirty = true;
      index_->SetSegmentOverflow(segment_base, true);

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
      // The segment is full. We need to rewrite it in order to merge in the
      // writes.
      if (options_.use_segments) {
        RewriteSegments(segment_base, records.begin() + i,
                        records.begin() + end_idx);
      } else {
        FlattenChain(segment_base, records.begin() + i,
                     records.begin() + end_idx);
      }
      return Status::OK();
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
  w_.BumpWriteCount(1);
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
          [this, otr]() { ReadPage(otr.first, 0, otr.second); }));
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

std::pair<Key, Key> Manager::GetPageBoundsFor(const Key key) const {
  const auto [seg_base_key, sinfo] = index_->SegmentForKey(key);
  const size_t page_idx = sinfo.PageForKey(seg_base_key, key);

  // Find the lower boundary. When `page_idx == 0` it is always the segment's
  // base key.
  Key lower_bound = seg_base_key;
  if (page_idx > 0) {
    assert(sinfo.page_count() > 1);
    lower_bound =
        FindLowerBoundary(seg_base_key, *(sinfo.model()), sinfo.page_count(),
                          sinfo.model()->Invert(), page_idx);
  }

  // Find upper boundary (exclusive).
  Key upper_bound = std::numeric_limits<Key>::max();
  if (page_idx < sinfo.page_count() - 1) {
    // `page_idx` is not the last page in the segment. Thus its upper boundary
    // is the lower boundary of the next page.
    upper_bound =
        FindLowerBoundary(seg_base_key, *(sinfo.model()), sinfo.page_count(),
                          sinfo.model()->Invert(), page_idx + 1);

  } else {
    // `page_idx` is the last page in the segment. Its upper boundary is the
    // next segment's base key (or the largest possible key if this is the last
    // segment).
    // TODO: Concurrency concerns.
    const auto maybe_next = index_->NextSegmentForKey(key);
    if (maybe_next.has_value()) {
      upper_bound = maybe_next->first;
    }
  }

  return {lower_bound, upper_bound};
}

}  // namespace pg
}  // namespace llsm
