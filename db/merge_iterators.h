#include <memory>
#include <optional>
#include <queue>

#include "db/db_impl.h"

namespace llsm {

// A k-way merging iterator for `Page` iterators. This iterator is different
// from the `MemTableMergeIterator` because there is no prioritization among the
// iterators; all records will be returned (in ascending order).
class PageMergeIterator {
 public:
  explicit PageMergeIterator(const DBImpl::OverflowChain& page_chain,
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

}  // namespace llsm