#pragma once

#include <cassert>
#include <cmath>
#include <optional>

namespace tl {
namespace pg {
namespace plr {

template <typename T>
class Point {
 public:
  Point(const T x, const T y) : x_(x), y_(y) {}
  Point() : Point(0, 0) {}

  T x() const { return x_; }
  T y() const { return y_; }

  Point<T> AddDelta(const T delta) const { return Point(x_, y_ + delta); }

 private:
  T x_, y_;
};

template <typename T>
class Line {
 public:
  Line(const T slope, const T intercept)
      : slope_(slope), intercept_(intercept) {}

  static Line<T> FromSlopeAndPoint(const T slope, const Point<T>& point) {
    // intercept = y - slope * x
    return Line<T>(slope, std::fma(-slope, point.x(), point.y()));
  }

  static Line<T> FromTwoPoints(const Point<T>& p1, const Point<T>& p2) {
    const T slope = (p2.y() - p1.y()) / (p2.x() - p1.x());
    return Line<T>::FromSlopeAndPoint(slope, p1);
  }

  T slope() const { return slope_; }
  T intercept() const { return intercept_; }

  T operator()(const T x) const { return std::fma(slope_, x, intercept_); }

  std::optional<Point<T>> Intersect(const Line<T>& other) const {
    if (slope_ == other.slope_) {
      // Either the lines are the same (infinite number of intersections) or
      // they are parallel (no intersections).
      return std::optional<Point<T>>();
    }
    const T x = (other.intercept_ - intercept_) / (slope_ - other.slope_);
    return Point<T>(x, (*this)(x));
  }

  // Divides the line's output range by `factor`.
  Line<T> Rescale(const T factor) const {
    assert(factor != 0);
    return Line<T>(slope_ / factor, intercept_ / factor);
  }

  Line<T> Invert() const {
    // x = 1 / slope_ * y - intercept_ / slope_
    return Line<T>(1.0 / slope_, -intercept_ / slope_);
  }

  bool operator==(const Line<T>& other) const {
    // N.B. Exact equality.
    return slope_ == other.slope_ && intercept_ == other.intercept_;
  }

 private:
  // y = slope_ * x + intercept_;
  T slope_, intercept_;
};

template <typename T>
class BoundedLine {
 public:
  BoundedLine(Line<T> line, const T start_x, const T end_x)
      : line_(std::move(line)), start_(start_x), end_(end_x) {
    assert(start_x <= end_x);
  }

  T start() const { return start_; }
  T end() const { return end_; }
  const Line<T>& line() const { return line_; }

 private:
  Line<T> line_;
  T start_, end_;
};

using Point64 = Point<double>;
using Line64 = Line<double>;
using BoundedLine64 = BoundedLine<double>;

}  // namespace plr
}  // namespace pg
}  // namespace tl
