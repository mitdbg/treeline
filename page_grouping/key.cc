#include "key.h"

#include <iterator>

#include "treeline/pg_db.h"

using namespace tl;
using namespace tl::pg;

namespace {

// Defined to run `std::lower_bound()` on the `Key` domain without materializing
// the values into an array.
class KeyDomainIterator
    : public std::iterator<std::random_access_iterator_tag, Key> {
 public:
  KeyDomainIterator() : value_(0) {}
  KeyDomainIterator(Key value) : value_(value) {}

  Key operator*() const { return value_; }

  // NOTE: This iterator does not define all the operators needed for a
  // "RandomAccessIterator". It just defines the operators used by
  // `std::lower_bound()`.

  KeyDomainIterator& operator++() {
    ++value_;
    return *this;
  }
  KeyDomainIterator& operator--() {
    --value_;
    return *this;
  }
  KeyDomainIterator& operator+=(size_t delta) {
    value_ += delta;
    return *this;
  }
  bool operator==(const KeyDomainIterator& other) const {
    return value_ == other.value_;
  }
  size_t operator-(const KeyDomainIterator& it) const {
    return value_ - it.value_;
  }

 private:
  Key value_;
};

}  // namespace

namespace tl {
namespace pg {

Key FindLowerBoundary(const Key base_key, const plr::Line64& model,
                      const size_t page_count, const plr::Line64& model_inv,
                      size_t page_idx) {
  // Strategy: To avoid precision errors, we binary search on the key space to
  // find the smallest possible key that will be assigned to the page. We use
  // the `model_inv` function to compute search bounds (it maps page indices to
  // keys).

  // 1. Use the inverted model to compute a candidate boundary key. We should
  // not use this key directly due to possible precision errors. Instead, we
  // use it to establish a search bound.
  const Key candidate_boundary =
      static_cast<Key>(std::max(0.0, model_inv(page_idx))) + base_key;
  const size_t page_for_candidate =
      PageForKey(base_key, model, page_count, candidate_boundary);

  // 2. Compute lower/upper bounds for the search space.
  Key lower = 0, upper = 0;
  if (page_for_candidate >= page_idx) {
    // `candidate_boundary` is an upper bound for the search space.
    // NOTE: This assumes that `model_inv(page_idx - 1)` produces a strictly
    // lower key.
    lower = static_cast<Key>(std::max(0.0, model_inv(page_idx - 1))) + base_key;
    upper = candidate_boundary;
  } else {
    // `candidate_boundary` is a lower bound for the search space.
    // NOTE: This assumes that `model_inv(page_idx + 1)` produces a strictly
    // higher key.
    lower = candidate_boundary;
    upper = static_cast<Key>(std::max(0.0, model_inv(page_idx + 1))) + base_key;
  }
  assert(lower < upper);

  // 3. Binary search over the search space to find the smallest key that maps
  // to `page_idx`.
  KeyDomainIterator lower_it(lower), upper_it(upper);
  const auto bound_it =
      std::lower_bound(lower_it, upper_it, page_idx,
                       [&base_key, &model, &page_count](const Key key_candidate,
                                                        const size_t page_idx) {
                         return PageForKey(base_key, model, page_count,
                                           key_candidate) < page_idx;
                       });
  // The boundary key maps to `page_idx`.
  assert(PageForKey(base_key, model, page_count, *bound_it) == page_idx);
  // The boundary key is the smallest key that maps to `page_idx`.
  assert(*bound_it == 0 ||
         PageForKey(base_key, model, page_count, (*bound_it) - 1) < page_idx);

  return *bound_it;
}

}  // namespace pg
}  // namespace tl
