#pragma once

#include <functional>
#include <queue>
#include <vector>

#include "llsm/slice.h"
#include "page.h"

namespace llsm {
namespace pg {

class PageMergeIterator {
 public:
  // Represents an empty iterator.
  PageMergeIterator()
      : merged_iterators_(std::bind(&PageMergeIterator::Compare, this,
                                    std::placeholders::_1,
                                    std::placeholders::_2)) {}

  explicit PageMergeIterator(std::vector<Page::Iterator> iterators,
                             const Slice* start_key = nullptr)
      : page_iterators_(std::move(iterators)),
        merged_iterators_(std::bind(&PageMergeIterator::Compare, this,
                                    std::placeholders::_1,
                                    std::placeholders::_2)) {
    for (size_t i = 0; i < page_iterators_.size(); ++i) {
      auto& it = page_iterators_[i];
      if (start_key != nullptr) it.Seek(*start_key);
      if (!it.Valid()) continue;
      merged_iterators_.push(i);
    }
  }

  bool Valid() const { return !merged_iterators_.empty(); }

  // REQUIRES: `Valid()` is true.
  void Next() {
    assert(Valid());
    const size_t it_idx = merged_iterators_.top();
    merged_iterators_.pop();

    auto& it = page_iterators_[it_idx];
    assert(it.Valid());
    it.Next();
    if (!it.Valid()) return;
    merged_iterators_.push(it_idx);
  }

  // REQUIRES: `Valid()` is true.
  Slice key() const {
    const size_t it_idx = merged_iterators_.top();
    const auto& it = page_iterators_[it_idx];
    assert(Valid() && it.Valid());
    return it.key();
  }

  // REQUIRES: `Valid()` is true.
  Slice value() const {
    const size_t it_idx = merged_iterators_.top();
    const auto& it = page_iterators_[it_idx];
    assert(Valid() && it.Valid());
    return it.value();
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
  bool Compare(const size_t left_it_idx, const size_t right_it_idx) {
    const auto& left = page_iterators_[left_it_idx];
    const auto& right = page_iterators_[right_it_idx];
    assert(left.Valid() && right.Valid());
    // Evaluates to true iff `left.key()` is greater than `right.key()`.
    return left.key().compare(right.key()) > 0;
  }

  std::vector<Page::Iterator> page_iterators_;
  std::priority_queue<size_t, std::vector<size_t>,
                      std::function<bool(size_t, size_t)>>
      merged_iterators_;
};

}  // namespace pg
}  // namespace llsm
