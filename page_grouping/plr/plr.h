#pragma once

#include <optional>

#include "data.h"

namespace tl {
namespace pg {
namespace plr {

// Common interface used by our PLR algorithms.
template <typename T>
class PLRBuilder {
 public:
  virtual ~PLRBuilder() = default;

  // Extend the segment to include `p`. If the return value is not empty, it
  // will include a bounded line segment that is part of the piecewise linear
  // regression.
  virtual std::optional<BoundedLine<T>> Offer(const Point<T>& p) = 0;

  // Retrieve the final built line.
  virtual std::optional<BoundedLine<T>> Finish() const = 0;
};

}  // namespace plr
}  // namespace pg
}  // namespace tl
