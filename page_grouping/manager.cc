#include "manager.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>

#include "bufmgr/page_memory_allocator.h"
#include "key.h"
#include "persist/merge_iterator.h"
#include "persist/page.h"
#include "persist/segment_file.h"
#include "persist/segment_wrap.h"
#include "segment_builder.h"
#include "treeline/pg_db.h"
#include "treeline/pg_stats.h"
#include "util/key.h"

namespace fs = std::filesystem;

namespace tl {
namespace pg {

using SegmentMode = LockManager::SegmentMode;
using PageMode = LockManager::PageMode;

thread_local Workspace Manager::w_;
const std::string Manager::kSegmentFilePrefix = "sf-";

Manager::Manager(fs::path db_path,
                 std::vector<std::pair<Key, SegmentInfo>> boundaries,
                 std::vector<std::unique_ptr<SegmentFile>> segment_files,
                 PageGroupedDBOptions options, uint32_t next_sequence_number,
                 std::unique_ptr<FreeList> free)
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
    bg_threads_ = std::make_unique<ThreadPool>(options_.num_bg_threads, []() {
      // Make sure all locally recorded stats are exposed.
      PageGroupedDBStats::Local().PostToGlobal();
    });
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
      /*num_pages=*/SegmentBuilder::SegmentPageCounts().back());
  Page first_page(buf.get());

  std::vector<std::unique_ptr<SegmentFile>> segment_files;
  std::vector<std::pair<Key, SegmentInfo>> segment_boundaries;
  std::unique_ptr<FreeList> free = std::make_unique<FreeList>();
  uint32_t max_sequence = 0;

  for (size_t i = 0; i < SegmentBuilder::SegmentPageCounts().size(); ++i) {
    if (i > 0 && !uses_segments) break;
    const size_t pages_per_segment = SegmentBuilder::SegmentPageCounts()[i];
    segment_files.push_back(std::make_unique<SegmentFile>(
        db / (kSegmentFilePrefix + std::to_string(i)), pages_per_segment,
        options.use_memory_based_io));
    std::unique_ptr<SegmentFile>& sf = segment_files.back();

    const size_t num_segments = sf->NumAllocatedSegments();
    const size_t bytes_per_segment = pages_per_segment * Page::kSize;
    for (size_t seg_idx = 0; seg_idx < num_segments; ++seg_idx) {
      // Offset in `id` is the page offset.
      SegmentId id(i, seg_idx * pages_per_segment);
      sf->ReadPages(seg_idx * bytes_per_segment, buf.get(), pages_per_segment);

      SegmentWrap sw(buf.get(), pages_per_segment);
      if (!sw.CheckChecksum() || !first_page.IsValid()) {
        // This segment is a "hole" in the file and can be reused.
        free->Add(id);
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

void Manager::SetTracker(std::shared_ptr<InsertTracker> tracker) {
  tracker_ = std::move(tracker);
}

Status Manager::Get(const Key& key, std::string* value_out) {
  return GetWithPages(key, value_out).first;
}

std::pair<Status, std::vector<pg::Page>> Manager::GetWithPages(
    const Key& key, std::string* value_out) {
  void* main_page_buf = w_.buffer().get();
  void* overflow_page_buf = w_.buffer().get() + pg::Page::kSize;

  // 1. Find the segment that should hold the key.
  const auto seg = index_->SegmentForKeyWithLock(key, SegmentMode::kPageRead);

  // 2. Figure out the page offset, lock the page, and then read it in.
  const size_t page_idx = seg.sinfo.PageForKey(seg.lower, key);
  lock_manager_->AcquirePageLock(seg.sinfo.id(), page_idx, PageMode::kShared);
  ReadPage(seg.sinfo.id(), page_idx, main_page_buf);

  // 3. Search for the record on the page.
  pg::Page main_page(main_page_buf);
  key_utils::IntKeyAsSlice key_slice(key);
  auto status = main_page.Get(key_slice.as<Slice>(), value_out);
  if (status.ok()) {
    lock_manager_->ReleasePageLock(seg.sinfo.id(), page_idx, PageMode::kShared);
    lock_manager_->ReleaseSegmentLock(seg.sinfo.id(), SegmentMode::kPageRead);
    return {status, {main_page}};
  }

  // 4. Check the overflow page if it exists.
  // TODO: We always assume at most 1 overflow page.
  if (!main_page.HasOverflow()) {
    lock_manager_->ReleasePageLock(seg.sinfo.id(), page_idx, PageMode::kShared);
    lock_manager_->ReleaseSegmentLock(seg.sinfo.id(), SegmentMode::kPageRead);
    return {Status::NotFound("Record does not exist."), {main_page}};
  }
  const SegmentId overflow_id = main_page.GetOverflow();
  // All overflow pages are single pages.
  assert(overflow_id.GetFileId() == 0);
  ReadPage(overflow_id, /*page_idx=*/0, overflow_page_buf);
  pg::Page overflow_page(overflow_page_buf);
  status = overflow_page.Get(key_slice.as<Slice>(), value_out);

  lock_manager_->ReleasePageLock(seg.sinfo.id(), page_idx, PageMode::kShared);
  lock_manager_->ReleaseSegmentLock(seg.sinfo.id(), SegmentMode::kPageRead);
  return {status, {main_page, overflow_page}};
}

Status Manager::PutBatch(const std::vector<std::pair<Key, Slice>>& records) {
  return PutBatchImpl(records, 0, records.size());
}

Status Manager::PutBatchImpl(const std::vector<std::pair<Key, Slice>>& records,
                             const size_t start_idx, const size_t end_idx) {
  static constexpr size_t kMaxAttempts = 1000;

  if (start_idx >= end_idx) return Status::OK();
  // TODO: Support deletes.
  size_t left_idx = start_idx;
  size_t num_attempts = 0;
  while (left_idx < end_idx) {
    ++num_attempts;
    const auto segment = index_->SegmentForKeyWithLock(records[left_idx].first,
                                                       SegmentMode::kPageWrite);
    const auto left_it = records.begin() + left_idx;
    const auto end_it = records.begin() + end_idx;
    const auto cutoff_it =
        std::lower_bound(left_it, end_it, segment.upper,
                         [](const auto& rec, const Key next_segment_start) {
                           return rec.first < next_segment_start;
                         });
    const size_t range_size = cutoff_it - left_it;
    // `WriteToSegment()` will release the segment lock.
    const size_t num_written =
        WriteToSegment(segment, records, left_idx, left_idx + range_size);
    // If a reorg intervenes and no records are written, `num_written` will
    // be 0 and the logic in this loop will retry the write.
    left_idx += num_written;

    if (num_written == 0 && num_attempts >= kMaxAttempts) {
      // This is a defensive check. If the number of attempts exceeds
      // `kMaxAttempts`, it usually indicates a bug in the segment write path.
      // Throwing the exception below prevents the DB from looping forever.
      std::stringstream err_msg;
      err_msg << "Exceeded the maximum number of write attempts for a batch "
                 "starting at key 0x"
              << std::hex << std::uppercase << records[left_idx].first
              << std::nouppercase << ". Segment lower: 0x" << std::uppercase
              << segment.lower << std::nouppercase << " Segment upper: 0x"
              << std::uppercase << segment.upper << std::nouppercase
              << std::dec;
      throw std::runtime_error(err_msg.str());

    } else if (num_written > 0) {
      // We count repeated attempts for records that go to the same segment.
      num_attempts = 0;
    }
  }
  return Status::OK();
}

Status Manager::PutBatchParallel(
    const std::vector<std::pair<Key, Slice>>& records) {
  if (bg_threads_ == nullptr) {
    // No background workers available; just fall back to a synchronous write.
    return PutBatchImpl(records, 0, records.size());
  }

  // TODO: Support deletes.
  // TODO: This parallelization strategy can lead to a synchronization deadlock
  // if `PutBatchImpl()` requires a reorg. This is because the segment rewrite
  // logic uses background threads to issue I/O in parallel, which shares the
  // same thread pool.
  std::vector<std::future<void>> write_futures;
  size_t left_idx = 0;
  while (left_idx < records.size()) {
    const auto [_, upper] = GetPageBoundsFor(records[left_idx].first);
    const auto left_it = records.begin() + left_idx;
    const auto cutoff_it =
        std::lower_bound(left_it, records.end(), upper,
                         [](const auto& rec, const Key next_page_start) {
                           return rec.first < next_page_start;
                         });
    const size_t range_size = cutoff_it - left_it;
    write_futures.push_back(bg_threads_->Submit(
        [this, &records, start = left_idx, end = left_idx + range_size]() {
          PutBatchImpl(records, start, end);
        }));
    left_idx += range_size;
  }

  // Wait for the writes to complete.
  for (auto& f : write_futures) {
    f.get();
  }

  // `PutBatchImpl()` always returns this.
  return Status::OK();
}

size_t Manager::WriteToSegment(
    const SegmentIndex::Entry& segment,
    const std::vector<std::pair<Key, Slice>>& records, const size_t start_idx,
    const size_t end_idx) {
  // Simplifying assumptions:
  // - If the number of writes is past a certain threshold
  //   (`record_per_page_goal` x `pages_in_segment` x 2), we don't attempt to
  //   make the writes. We immediately trigger a segment reorg.
  // - Records are the same size.
  // - No deletes. So it's always safe to call `Put()` on the page.
  // - As soon as we try to insert into a full overflow page, we trigger a
  //   segment reorg.

  const size_t reorg_threshold =
      options_.records_per_page_goal * segment.sinfo.page_count() * 2ULL;
  if (end_idx - start_idx > reorg_threshold) {
    // Immediately trigger a segment rewrite that merges in the records.
    lock_manager_->ReleaseSegmentLock(segment.sinfo.id(),
                                      SegmentMode::kPageWrite);
    Status status;
    if (options_.use_segments) {
      status = RewriteSegments(segment.lower, records.begin() + start_idx,
                               records.begin() + end_idx);
    } else {
      status = FlattenChain(segment.lower, records.begin() + start_idx,
                            records.begin() + end_idx);
    }
    // If the rewrite succeeded then all of the records will have been written
    // into the new segments.
    return status.ok() ? (end_idx - start_idx) : 0;
  }

  void* orig_page_buf = w_.buffer().get();
  void* overflow_page_buf = w_.buffer().get() + pg::Page::kSize;
  pg::Page orig_page(orig_page_buf);
  pg::Page overflow_page(overflow_page_buf);

  size_t curr_page_idx = 0;

  SegmentId overflow_page_id;
  bool orig_page_dirty = false;
  bool overflow_page_dirty = false;

  pg::Page* curr_page = &orig_page;
  bool* curr_page_dirty = &orig_page_dirty;

  auto write_dirty_pages = [&, segment_base = segment.lower,
                            sinfo = segment.sinfo]() {
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
    if (curr_page == &overflow_page || options_.disable_overflow_creation) {
      // Cannot allocate another overflow (reached max length, or overflow
      // creation was disabled).
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
      const auto maybe_free_page = free_->Get(/*page_count=*/1);
      if (maybe_free_page.has_value()) {
        overflow_page_id = *maybe_free_page;
      } else {
        // Allocate a new free page.
        const size_t byte_offset = segment_files_[0]->AllocateSegment();
        overflow_page_id = SegmentId(0, byte_offset / pg::Page::kSize);
      }

      memset(overflow_page_buf, 0, pg::Page::kSize);
      overflow_page = Page(overflow_page_buf, orig_page);
      overflow_page.MakeOverflow();
      overflow_page.SetOverflow(SegmentId());
      overflow_page_dirty = true;
      index_->SetSegmentOverflow(segment.lower, true);
      PageGroupedDBStats::Local().BumpOverflowsCreated();

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

  curr_page_idx =
      segment.sinfo.PageForKey(segment.lower, records[start_idx].first);
  lock_manager_->AcquirePageLock(segment.sinfo.id(), curr_page_idx,
                                 PageMode::kExclusive);
  ReadPage(segment.sinfo.id(), curr_page_idx, orig_page_buf);

  for (size_t i = start_idx; i < end_idx; ++i) {
    const size_t page_idx =
        segment.sinfo.PageForKey(segment.lower, records[i].first);
    if (page_idx != curr_page_idx) {
      write_dirty_pages();
      lock_manager_->ReleasePageLock(segment.sinfo.id(), curr_page_idx,
                                     PageMode::kExclusive);
      // Update the current page.
      curr_page_idx = page_idx;
      lock_manager_->AcquirePageLock(segment.sinfo.id(), curr_page_idx,
                                     PageMode::kExclusive);
      ReadPage(segment.sinfo.id(), curr_page_idx, orig_page_buf);
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
      lock_manager_->ReleasePageLock(segment.sinfo.id(), curr_page_idx,
                                     PageMode::kExclusive);
      lock_manager_->ReleaseSegmentLock(segment.sinfo.id(),
                                        SegmentMode::kPageWrite);
      // The segment is full. We need to rewrite it in order to merge in the
      // writes.
      Status status;
      if (options_.use_segments) {
        status = RewriteSegments(segment.lower, records.begin() + i,
                                 records.begin() + end_idx);
      } else {
        status = FlattenChain(segment.lower, records.begin() + i,
                              records.begin() + end_idx);
      }

      // If the rewrite succeeded then all the records will have been written
      // into the new segments. Otherwise only the records we processed up to
      // index `i` (but excluding `i`) will have been written.
      return status.ok() ? (end_idx - start_idx) : (i - start_idx);
    }
  }

  write_dirty_pages();
  lock_manager_->ReleasePageLock(segment.sinfo.id(), curr_page_idx,
                                 PageMode::kExclusive);
  lock_manager_->ReleaseSegmentLock(segment.sinfo.id(),
                                    SegmentMode::kPageWrite);

  // All the records were successfully written.
  return end_idx - start_idx;
}

void Manager::ReadPage(const SegmentId& seg_id, size_t page_idx,
                       void* buffer) const {
  assert(seg_id.IsValid());
  const std::unique_ptr<SegmentFile>& sf = segment_files_[seg_id.GetFileId()];
  sf->ReadPages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
                /*num_pages=*/1);
  w_.BumpReadCount(1);
}

void Manager::WritePage(const SegmentId& seg_id, size_t page_idx,
                        void* buffer) const {
  assert(seg_id.IsValid());
  const std::unique_ptr<SegmentFile>& sf = segment_files_[seg_id.GetFileId()];
  sf->WritePages((seg_id.GetOffset() + page_idx) * pg::Page::kSize, buffer,
                 /*num_pages=*/1);
  w_.BumpWriteCount(1);
}

void Manager::ReadSegment(const SegmentId& seg_id) const {
  assert(seg_id.IsValid());
  const std::unique_ptr<SegmentFile>& sf = segment_files_[seg_id.GetFileId()];
  sf->ReadPages(seg_id.GetOffset() * pg::Page::kSize, w_.buffer().get(),
                sf->PagesPerSegment());
  w_.BumpReadCount(sf->PagesPerSegment());
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
  const auto seg = index_->SegmentForKey(key);
  const size_t page_idx = seg.sinfo.PageForKey(seg.lower, key);

  // Find the lower boundary. When `page_idx == 0` it is always the segment's
  // base key.
  Key lower_bound = seg.lower;
  if (page_idx > 0) {
    assert(seg.sinfo.page_count() > 1);
    lower_bound = FindLowerBoundary(seg.lower, *(seg.sinfo.model()),
                                    seg.sinfo.page_count(),
                                    seg.sinfo.model()->Invert(), page_idx);
  }

  // Find upper boundary (exclusive).
  Key upper_bound = std::numeric_limits<Key>::max();
  if (page_idx < seg.sinfo.page_count() - 1) {
    // `page_idx` is not the last page in the segment. Thus its upper boundary
    // is the lower boundary of the next page.
    upper_bound = FindLowerBoundary(seg.lower, *(seg.sinfo.model()),
                                    seg.sinfo.page_count(),
                                    seg.sinfo.model()->Invert(), page_idx + 1);

  } else {
    // `page_idx` is the last page in the segment. Its upper boundary is the
    // next segment's base key (or the largest possible key if this is the last
    // segment).
    upper_bound = seg.upper;
  }

  return {lower_bound, upper_bound};
}

void Manager::PostStats() const {
  PageGroupedDBStats::Local().SetFreeListBytes(free_->GetSizeFootprint());
  PageGroupedDBStats::Local().SetFreeListEntries(free_->GetNumEntries());
  PageGroupedDBStats::Local().SetSegmentIndexBytes(index_->GetSizeFootprint());
  PageGroupedDBStats::Local().SetSegments(index_->GetNumEntries());
}

}  // namespace pg
}  // namespace tl
