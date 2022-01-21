#pragma once

#include <algorithm>
#include <cstdint>

#include "plr/data.h"

namespace llsm {
namespace pg {

using Key = uint64_t;

// Returns an integer in the range [0, page_count) according to `model`.
inline size_t PageForKey(const Key base_key, const plr::Line64& model,
                         const size_t page_count, const Key candidate) {
  return std::min(page_count - 1, static_cast<size_t>(std::max(
                                      0.0, model(candidate - base_key))));
}

}  // namespace pg
}  // namespace llsm
