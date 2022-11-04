#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>

#include "treeline/pg_db.h"
#include "treeline/slice.h"
#include "plr/data.h"

namespace tl {
namespace pg {

// Returns an integer in the range [0, page_count) according to `model`.
inline size_t PageForKey(const Key base_key, const plr::Line64& model,
                         const size_t page_count, const Key candidate) {
  return std::min(page_count - 1, static_cast<size_t>(std::max(
                                      0.0, model(candidate - base_key))));
}

// Returns the smallest key `k` such that
// `PageForKey(base_key, model, page_count, k) == page_idx`.
//
// `model` should map keys to page indices. `model_inv` should be the result of
// `model.Invert()`.
Key FindLowerBoundary(const Key base_key, const plr::Line64& model,
                      const size_t page_count, const plr::Line64& model_inv,
                      size_t page_idx);

}  // namespace pg
}  // namespace tl
