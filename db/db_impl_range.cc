#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

#include "bufmgr/buffer_frame.h"
#include "db/db_impl.h"
#include "db/format.h"
#include "db/page.h"

namespace {

using namespace llsm;

// A special merging iterator that handles iteration over both the active
// memtable and immutable (being flushed) memtable. This iterator helps ensure
// the latest record value is returned (i.e., it prioritizes records from the
// active memtable because they are newer).
class MemTableMergeIterator {
 public:
  MemTableMergeIterator(const std::shared_ptr<MemTable>& active_mtable,
                        const std::shared_ptr<MemTable>& immutable_mtable,
                        const Slice& start_key)
      : compare_(-1),
        active_(active_mtable->GetIterator()),
        flushing_(immutable_mtable != nullptr
                      ? immutable_mtable->GetIterator()
                      : std::optional<MemTable::Iterator>()) {
    active_.Seek(start_key);
    if (flushing_.has_value()) {
      flushing_->Seek(start_key);
    }
    UpdateCompare();
  }

  bool Valid() const {
    return active_.Valid() || (flushing_.has_value() && flushing_->Valid());
  }

  // REQUIRES: `Valid()` is true.
  void Next() {
    assert(Valid());
    // N.B.: When `compare_ == 0`, both iterators are incremented.
    if (compare_ <= 0) {
      active_.Next();
    }
    if (compare_ >= 0) {
      // If `compare_` is non-negative, `flushing_` must exist.
      flushing_->Next();
    }
    UpdateCompare();
  }

  // REQUIRES: `Valid()` is true.
  Slice key() const {
    assert(Valid());
    return compare_ <= 0 ? active_.key() : flushing_->key();
  }

  // REQUIRES: `Valid()` is true.
  Slice value() const {
    assert(Valid());
    return compare_ <= 0 ? active_.value() : flushing_->value();
  }

  // REQUIRES: `Valid()` is true.
  format::WriteType type() const {
    assert(Valid());
    return compare_ <= 0 ? active_.type() : flushing_->type();
  }

 private:
  void UpdateCompare() {
    if (!Valid()) return;

    if (!flushing_.has_value()) {
      // There is no being-flushed memtable. All records should come from
      // `active_`.
      compare_ = -1;
      return;
    }

    if (active_.Valid() && !flushing_->Valid()) {
      // No more records in `flushing_`. All records should come from `active_`.
      compare_ = -1;
    } else if (!active_.Valid() && flushing_->Valid()) {
      // No more records in `active_`. All records should come from `flushing_`.
      compare_ = 1;
    } else {
      // Actually compare the keys from each iterator.
      compare_ = active_.key().compare(flushing_->key());
    }
  }

  // The comparison value between the active_ iterator's current key and the
  // immutable_ iterator's current key.
  // < 0 implies active_ key < flushing_ key (or flushing_ is empty)
  // = 0 implies active_ key = flushing_ key
  // > 0 implies active_ key > flushing_ key
  int compare_;
  MemTable::Iterator active_;
  // If there is no being-flushed memtable, this optional will be empty.
  std::optional<MemTable::Iterator> flushing_;
};

// A k-way merging iterator for `Page` iterators. This iterator is different
// from the `MemTableMergeIterator` because there is no prioritization among the
// iterators; all records will be returned (in ascending order).
class PageMergeIterator {
 public:
  explicit PageMergeIterator(
      const std::unique_ptr<std::vector<BufferFrame*>>& page_chain,
      const Slice* start_key = nullptr)
      : merged_iterators_(&PageMergeIterator::Compare) {
    page_iterators_.reserve(page_chain->size());
    for (auto& bf : *page_chain) {
      page_iterators_.emplace_back(bf->GetPage().GetIterator());
    }
    for (auto& it : page_iterators_) {
      if (start_key != nullptr) it.Seek(*start_key);
      if (!it.Valid()) continue;
      merged_iterators_.push(&it);
    }
  }

  bool Valid() const { return !merged_iterators_.empty(); }

  // REQUIRES: `Valid()` is true.
  void Next() {
    assert(Valid());
    Page::Iterator* const it = merged_iterators_.top();
    merged_iterators_.pop();

    assert(it->Valid());
    it->Next();
    if (!it->Valid()) return;
    merged_iterators_.push(it);
  }

  // REQUIRES: `Valid()` is true.
  Slice key() const {
    assert(Valid() && merged_iterators_.top()->Valid());
    return merged_iterators_.top()->key();
  }

  // REQUIRES: `Valid()` is true.
  Slice value() const {
    assert(Valid() && merged_iterators_.top()->Valid());
    return merged_iterators_.top()->value();
  }

 private:
  // This is a comparison function for the priority queue. This function is
  // supposed to return true if `left` is strictly smaller than `right`.
  // However, `std::priority_queue` returns the *largest* items first, whereas
  // we want to return the smallest items first. So we return true here if
  // `left` is strictly larger than `right` to ensure the smallest records are
  // returned first.
  static bool Compare(const Page::Iterator* left, const Page::Iterator* right) {
    assert(left->Valid() && right->Valid());
    // Evaluates to true iff `left->key()` is greater than `right->key()`.
    return left->key().compare(right->key()) > 0;
  }

  std::vector<Page::Iterator> page_iterators_;
  std::priority_queue<Page::Iterator*, std::vector<Page::Iterator*>,
                      decltype(&PageMergeIterator::Compare)>
      merged_iterators_;
};

}  // namespace

namespace llsm {

Status DBImpl::GetRange(const ReadOptions& options, const Slice& start_key,
                        const size_t num_records, RecordBatch* results_out) {
  // We need hold a copy of the memtable shared pointers to ensure the memtables
  // do not get deleted while we do the scan.
  std::shared_ptr<MemTable> local_mtable = nullptr;
  std::shared_ptr<MemTable> local_im_mtable = nullptr;
  {
    std::unique_lock<std::mutex> mtable_lock(mtable_mutex_);
    local_mtable = mtable_;
    local_im_mtable = im_mtable_;
  }

  MemTable::Iterator active = local_mtable->GetIterator();
  std::optional<MemTable::Iterator> immutable =
      local_im_mtable != nullptr ? local_im_mtable->GetIterator()
                                 : std::optional<MemTable::Iterator>();

  MemTableMergeIterator mtable_it(local_mtable, local_im_mtable, start_key);

  results_out->clear();
  results_out->reserve(num_records);

  assert(buf_mgr_->GetFileManager() != nullptr);
  const size_t total_db_pages = buf_mgr_->GetFileManager()->GetNumPages();
  PhysicalPageId curr_page_id = model_->KeyToPageId(start_key);
  bool is_first_page = true;

  while (results_out->size() < num_records && curr_page_id.IsValid()) {
    // We need to retrieve the page chain first because it may have a smaller
    // key than the key at the current position of the memtable iterator.
    const std::unique_ptr<std::vector<BufferFrame*>> page_chain =
        FixOverflowChain(curr_page_id, /*exclusive=*/false,
                         /*unlock_before_returning=*/false);

    PageMergeIterator page_it(page_chain, is_first_page ? &start_key : nullptr);
    if (is_first_page) {
      is_first_page = false;
    }

    // Merge the memtable results with the page results, prioritizing the
    // memtable(s) for records with the same key.
    while (results_out->size() < num_records && mtable_it.Valid() &&
           page_it.Valid()) {
      const int compare = mtable_it.key().compare(page_it.key());
      if (compare <= 0) {
        // We do not emit the record if it was deleted.
        if (mtable_it.type() == format::WriteType::kWrite) {
          results_out->emplace_back(mtable_it.key().ToString(),
                                    mtable_it.value().ToString());
        }
        if (compare == 0) {
          page_it.Next();
        }
        mtable_it.Next();

      } else {
        // The page has the smaller record.
        results_out->emplace_back(page_it.key().ToString(),
                                  page_it.value().ToString());
        page_it.Next();
      }
    }

    // This loop only runs if `mtable_it` has no remaining records.
    while (results_out->size() < num_records && page_it.Valid()) {
      results_out->emplace_back(page_it.key().ToString(),
                                page_it.value().ToString());
      page_it.Next();
    }

    // We're finished reading this page chain.
    for (auto& bf : *page_chain) {
      buf_mgr_->UnfixPage(*bf, /*is_dirty=*/false);
    }

    curr_page_id = model_->KeyToNextPageId(
        page_chain->at(0)->GetPage().GetLowerBoundary());
  }

  // No more pages to check. If we still need to read more records, read the
  // rest of the records in the memtable(s) (if any are left).
  while (results_out->size() < num_records && mtable_it.Valid()) {
    if (mtable_it.type() == format::WriteType::kWrite) {
      results_out->emplace_back(mtable_it.key().ToString(),
                                mtable_it.value().ToString());
    }
    mtable_it.Next();
  }

  return Status::OK();
}

}  // namespace llsm
