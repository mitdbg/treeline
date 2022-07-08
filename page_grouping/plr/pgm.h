#pragma once

#include "plr.h"
#include "third_party/pgm/piecewise_linear_model.hpp"

namespace tl {
namespace pg {
namespace plr {

template <typename T>
class PGMBuilder : public PLRBuilder<T> {
 public:
  PGMBuilder(T epsilon);

  // Extend the segment to include `p`. If the return value is not empty, it
  // will include a bounded line segment that is part of the piecewise linear
  // regression.
  std::optional<BoundedLine<T>> Offer(const Point<T>& p) override;

  // Retrieve the final built line.
  std::optional<BoundedLine<T>> Finish() const override;

 private:
  BoundedLine<T> FromCanonicalSegment(
      const typename pgm::OptimalPiecewiseLinearModel<T, T>::CanonicalSegment&
          seg) const;

  pgm::OptimalPiecewiseLinearModel<T, T> pgm_;
};

// Implementation details follow.

template <typename T>
PGMBuilder<T>::PGMBuilder(T epsilon) : pgm_(epsilon) {}

template <typename T>
std::optional<BoundedLine<T>> PGMBuilder<T>::Offer(const Point<T>& p) {
  const bool succeeded = pgm_.add_point(p.x(), p.y());
  if (succeeded) return std::optional<BoundedLine<T>>();

  // We could not add the previous point without violating epsilon. Extract the
  // segment and then re-add the point. This second attempt will always succeed
  // (because it is the first point in a new segment).
  const auto segment = pgm_.get_segment();
  const bool succeeded2 = pgm_.add_point(p.x(), p.y());
  assert(succeeded2);

  return FromCanonicalSegment(segment);
}

template <typename T>
std::optional<BoundedLine<T>> PGMBuilder<T>::Finish() const {
  return FromCanonicalSegment(pgm_.get_segment());
}

template <typename T>
BoundedLine<T> PGMBuilder<T>::FromCanonicalSegment(
    const typename pgm::OptimalPiecewiseLinearModel<T, T>::CanonicalSegment&
        seg) const {
  const auto [slope, intercept] =
      seg.get_floating_point_segment(seg.get_first_x());
  return BoundedLine<T>(Line<T>(slope, intercept), seg.get_first_x(),
                        seg.get_last_x());
}

}  // namespace plr
}  // namespace pg
}  // namespace tl
