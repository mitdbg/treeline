#pragma once

#include <cassert>
#include <optional>

#include "data.h"
#include "plr.h"

namespace tl {
namespace pg {
namespace plr {

// An implementation of the GreedyPLR algorithm described in
//
//   Qing Xie, Chaoyi Pang, Xiaofang Zhou, Xiangliang Zhang, and Ke Deng. 2014.
//   Maximum error-bounded Piecewise Linear Representation for online stream
//   approximation. The VLDB Journal 23, 6 (December 2014), 915–937. DOI:
//   https://doi.org/10.1007/s00778-014-0355-0
//
// This class greedily consumes points until it can no longer form a line
// segment that has a maximum error of less than `delta` relative to each of
// the provided data points.
//
// N.B. `delta` is inclusive error. The segment will have an error of at most
// `delta`.
template <typename T>
class GreedyPLRSegmenter {
 public:
  GreedyPLRSegmenter(const Point<T>& s1, const Point<T>& s2, T delta);

  // Attempt to extend the segment to include `p`. Returns the resulting line if
  // `p` cannot be included without violating `delta`.
  std::optional<BoundedLine<T>> Offer(const Point<T>& p);

  // Retrieve the built line.
  BoundedLine<T> Finish() const;

  // Retrieve the most recently processed point.
  const Point<T>& last_point() const { return last_point_; }

 private:
  T ComputeSlope(const Point<T>& p1, const Point<T>& p2) const;

  Point<T> s0_, last_point_;
  T delta_;
  T start_x_;
  T slope_bot_, slope_top_;
};

// A convenience wrapper around `GreedyPLRSegment` that will return the
// constructed linear segments when presented with a stream of points.
//
// N.B. This algorithm does not produce connected line segments (i.e.,
// discontinuities across the linear functions are allowed).
template <typename T>
class GreedyPLRBuilder : public PLRBuilder<T> {
 public:
  // Perform piecewise linear regression with an error of at most `delta`.
  GreedyPLRBuilder(T delta);

  // Extend the segment to include `p`. If the return value is not empty, it
  // will include a bounded line segment that is part of the piecewise linear
  // regression.
  std::optional<BoundedLine<T>> Offer(const Point<T>& p) override;

  // Retrieve the final built line.
  std::optional<BoundedLine<T>> Finish() const override;

 private:
  T delta_;
  std::optional<Point<T>> s1_, s2_;
  std::optional<GreedyPLRSegmenter<T>> curr_;
};

using GreedyPLRBuilder64 = GreedyPLRBuilder<double>;

// Implementation details follow.

template <typename T>
inline GreedyPLRSegmenter<T>::GreedyPLRSegmenter(const Point<T>& s1,
                                                 const Point<T>& s2, T delta)
    : last_point_(s2), delta_(delta), start_x_(s1.x()) {
  assert(s1.x() < s2.x());
  assert(delta > 0);
  const auto s1_bot = s1.AddDelta(-delta);
  const auto s1_top = s1.AddDelta(delta);
  const auto s2_bot = s2.AddDelta(-delta);
  const auto s2_top = s2.AddDelta(delta);
  slope_bot_ = ComputeSlope(s1_top, s2_bot);
  slope_top_ = ComputeSlope(s1_bot, s2_top);
  const auto l1 = Line<T>::FromTwoPoints(s1_top, s2_bot);
  const auto l2 = Line<T>::FromTwoPoints(s1_bot, s2_top);
  // Since `delta` is nonzero and `s1.x() < s2.x()`, one unique intersection
  // must exist.
  const auto s0 = l1.Intersect(l2);
  s0_ = s0.value();
}

template <typename T>
inline std::optional<BoundedLine<T>> GreedyPLRSegmenter<T>::Offer(
    const Point<T>& p) {
  // Check if we will violate `delta` if we include this point.
  const auto bottom_line = Line<T>::FromSlopeAndPoint(slope_bot_, s0_);
  const T bottom_pred = bottom_line(p.x());
  if (bottom_pred > p.y()) {
    return Finish();
  }
  const auto top_line = Line<T>::FromSlopeAndPoint(slope_top_, s0_);
  const T top_pred = top_line(p.x());
  if (top_pred < p.y()) {
    return Finish();
  }

  // Adjust the slopes.
  const T x_diff = p.x() - s0_.x();
  const T y_diff = p.y() - s0_.y();
  if (std::abs(std::fma(-slope_bot_, x_diff, y_diff)) > delta_) {
    slope_bot_ = ComputeSlope(s0_, p.AddDelta(-delta_));
  }
  if (std::abs(std::fma(-slope_top_, x_diff, y_diff)) > delta_) {
    slope_top_ = ComputeSlope(s0_, p.AddDelta(delta_));
  }

  last_point_ = p;
  return std::optional<BoundedLine<T>>();
}

template <typename T>
inline BoundedLine<T> GreedyPLRSegmenter<T>::Finish() const {
  return BoundedLine<T>(
      Line<T>::FromSlopeAndPoint((slope_bot_ + slope_top_) / 2.0, s0_),
      start_x_, last_point_.x());
}

template <typename T>
inline T GreedyPLRSegmenter<T>::ComputeSlope(const Point<T>& p1,
                                             const Point<T>& p2) const {
  return (p2.y() - p1.y()) / (p2.x() - p1.x());
}

template <typename T>
inline GreedyPLRBuilder<T>::GreedyPLRBuilder(const T delta) : delta_(delta) {}

template <typename T>
inline std::optional<BoundedLine<T>> GreedyPLRBuilder<T>::Offer(
    const Point<T>& p) {
  if (!s1_.has_value()) {
    s1_ = p;
    return std::optional<BoundedLine<T>>();
  } else if (!s2_.has_value()) {
    s2_ = p;
    curr_ = GreedyPLRSegmenter<T>(*s1_, *s2_, delta_);
    return std::optional<BoundedLine<T>>();
  }

  const auto res = curr_->Offer(p);
  if (res.has_value()) {
    s1_ = curr_->last_point();
    s2_ = p;
    curr_ = GreedyPLRSegmenter<T>(*s1_, *s2_, delta_);
  }
  return res;
}

template <typename T>
inline std::optional<BoundedLine<T>> GreedyPLRBuilder<T>::Finish() const {
  if (!s1_.has_value()) {
    return std::optional<BoundedLine<T>>();
  } else if (!s2_.has_value()) {
    // Creates a horizontal line.
    return BoundedLine<T>(
        Line<T>::FromTwoPoints(*s1_, Point<T>(s1_->x() + 1, s1_->y())),
        s1_->x(), s1_->x() + 1);
  }
  return curr_->Finish();
}

}  // namespace plr
}  // namespace pg
}  // namespace tl
