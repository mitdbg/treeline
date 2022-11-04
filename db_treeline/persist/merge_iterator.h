#pragma once

#include <functional>
#include <queue>
#include <vector>

#include "treeline/slice.h"
#include "page.h"

namespace tl {
namespace pg {

class PageMergeIterator {
 public:
  // Represents an empty iterator.
  PageMergeIterator() : merged_iterators_(&PageMergeIterator::Compare) {}

  explicit PageMergeIterator(std::vector<Page::Iterator> iterators,
                             const Slice* start_key = nullptr)
      : page_iterators_(std::move(iterators)),
        merged_iterators_(&PageMergeIterator::Compare) {
    for (auto& it : page_iterators_) {
      if (start_key != nullptr) it.Seek(*start_key);
      if (!it.Valid()) continue;
      assert(it.Valid());
      merged_iterators_.push(&it);
    }
  }

  ~PageMergeIterator() = default;

  PageMergeIterator(const PageMergeIterator& other)
      : page_iterators_(other.page_iterators_),
        merged_iterators_(&PageMergeIterator::Compare) {
    InitHeap();
  }

  PageMergeIterator& operator=(const PageMergeIterator& other) {
    if (this == &other) return *this;
    page_iterators_ = other.page_iterators_;
    InitHeap();
    return *this;
  }

  PageMergeIterator(PageMergeIterator&& other)
      : page_iterators_(other.page_iterators_),
        merged_iterators_(&PageMergeIterator::Compare) {
    InitHeap();
  }

  PageMergeIterator& operator=(PageMergeIterator&& other) {
    if (this == &other) return *this;
    page_iterators_ = other.page_iterators_;
    InitHeap();
    return *this;
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
    assert(Valid());
    Page::Iterator* const it = merged_iterators_.top();
    assert(it->Valid());
    return it->key();
  }

  // REQUIRES: `Valid()` is true.
  Slice value() const {
    assert(Valid());
    Page::Iterator* const it = merged_iterators_.top();
    assert(it->Valid());
    return it->value();
  }

  size_t RecordsLeft() const {
    size_t records_left = 0;
    for (auto& it : page_iterators_) {
      records_left += it.RecordsLeft();
    }
    return records_left;
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
    // Evaluates to true iff `left.key()` is greater than `right.key()`.
    return left->key().compare(right->key()) > 0;
  }

  // Used by the copy/move constructors/assignment operators.
  void InitHeap() {
    while (!merged_iterators_.empty()) {
      merged_iterators_.pop();
    }
    for (auto& it : page_iterators_) {
      if (!it.Valid()) continue;
      merged_iterators_.push(&it);
    }
  }

  std::vector<Page::Iterator> page_iterators_;
  std::priority_queue<Page::Iterator*, std::vector<Page::Iterator*>,
                      decltype(&PageMergeIterator::Compare)>
      merged_iterators_;
};

}  // namespace pg
}  // namespace tl
