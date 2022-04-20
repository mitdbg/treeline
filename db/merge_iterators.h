#include <memory>
#include <optional>
#include <queue>

#include "db/db_impl.h"

namespace tl {

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
  explicit PageMergeIterator(const OverflowChain& page_chain,
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

}  // namespace tl
