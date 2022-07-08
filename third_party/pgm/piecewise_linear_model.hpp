// This file is part of PGM-index <https://github.com/gvinciguerra/PGM-index>.
// Copyright (c) 2018 Giorgio Vinciguerra.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace pgm {

template <typename T>
using LargeSigned = typename std::conditional_t<
    std::is_floating_point_v<T>, long double,
    std::conditional_t<(sizeof(T) < 8), int64_t, __int128>>;

template <typename X, typename Y>
class OptimalPiecewiseLinearModel {
 private:
  using SX = LargeSigned<X>;
  using SY = LargeSigned<Y>;

  struct Slope {
    SX dx{};
    SY dy{};

    bool operator<(const Slope& p) const { return dy * p.dx < dx * p.dy; }
    bool operator>(const Slope& p) const { return dy * p.dx > dx * p.dy; }
    bool operator==(const Slope& p) const { return dy * p.dx == dx * p.dy; }
    bool operator!=(const Slope& p) const { return dy * p.dx != dx * p.dy; }
    explicit operator long double() const { return dy / (long double)dx; }
  };

  struct Point {
    X x{};
    Y y{};

    Slope operator-(const Point& p) const { return {SX(x) - p.x, SY(y) - p.y}; }
  };

  const Y epsilon;
  std::vector<Point> lower;
  std::vector<Point> upper;
  X first_x = 0;
  X last_x = 0;
  size_t lower_start = 0;
  size_t upper_start = 0;
  size_t points_in_hull = 0;
  Point rectangle[4];

  auto cross(const Point& O, const Point& A, const Point& B) const {
    auto OA = A - O;
    auto OB = B - O;
    return OA.dx * OB.dy - OA.dy * OB.dx;
  }

 public:
  class CanonicalSegment;

  explicit OptimalPiecewiseLinearModel(Y epsilon)
      : epsilon(epsilon), lower(), upper() {
    if (epsilon < 0) throw std::invalid_argument("epsilon cannot be negative");

    upper.reserve(1u << 16);
    lower.reserve(1u << 16);
  }

  bool add_point(const X& x, const Y& y) {
    if (points_in_hull > 0 && x <= last_x)
      throw std::logic_error("Points must be increasing by x.");

    last_x = x;
    auto max_y = std::numeric_limits<Y>::max();
    auto min_y = std::numeric_limits<Y>::lowest();
    Point p1{x, y >= max_y - epsilon ? max_y : y + epsilon};
    Point p2{x, y <= min_y + epsilon ? min_y : y - epsilon};

    if (points_in_hull == 0) {
      first_x = x;
      rectangle[0] = p1;
      rectangle[1] = p2;
      upper.clear();
      lower.clear();
      upper.push_back(p1);
      lower.push_back(p2);
      upper_start = lower_start = 0;
      ++points_in_hull;
      return true;
    }

    if (points_in_hull == 1) {
      rectangle[2] = p2;
      rectangle[3] = p1;
      upper.push_back(p1);
      lower.push_back(p2);
      ++points_in_hull;
      return true;
    }

    auto slope1 = rectangle[2] - rectangle[0];
    auto slope2 = rectangle[3] - rectangle[1];
    bool outside_line1 = p1 - rectangle[2] < slope1;
    bool outside_line2 = p2 - rectangle[3] > slope2;

    if (outside_line1 || outside_line2) {
      points_in_hull = 0;
      return false;
    }

    if (p1 - rectangle[1] < slope2) {
      // Find extreme slope
      auto min = lower[lower_start] - p1;
      auto min_i = lower_start;
      for (auto i = lower_start + 1; i < lower.size(); i++) {
        auto val = lower[i] - p1;
        if (val > min) break;
        min = val;
        min_i = i;
      }

      rectangle[1] = lower[min_i];
      rectangle[3] = p1;
      lower_start = min_i;

      // Hull update
      auto end = upper.size();
      for (; end >= upper_start + 2 &&
             cross(upper[end - 2], upper[end - 1], p1) <= 0;
           --end)
        continue;
      upper.resize(end);
      upper.push_back(p1);
    }

    if (p2 - rectangle[0] > slope1) {
      // Find extreme slope
      auto max = upper[upper_start] - p2;
      auto max_i = upper_start;
      for (auto i = upper_start + 1; i < upper.size(); i++) {
        auto val = upper[i] - p2;
        if (val < max) break;
        max = val;
        max_i = i;
      }

      rectangle[0] = upper[max_i];
      rectangle[2] = p2;
      upper_start = max_i;

      // Hull update
      auto end = lower.size();
      for (; end >= lower_start + 2 &&
             cross(lower[end - 2], lower[end - 1], p2) >= 0;
           --end)
        continue;
      lower.resize(end);
      lower.push_back(p2);
    }

    ++points_in_hull;
    return true;
  }

  CanonicalSegment get_segment() const {
    if (points_in_hull == 1)
      return CanonicalSegment(rectangle[0], rectangle[1], first_x);
    return CanonicalSegment(rectangle, first_x);
  }

  void reset() {
    points_in_hull = 0;
    lower.clear();
    upper.clear();
  }
};

template <typename X, typename Y>
class OptimalPiecewiseLinearModel<X, Y>::CanonicalSegment {
  friend class OptimalPiecewiseLinearModel;

  Point rectangle[4];
  X first;

  CanonicalSegment(const Point& p0, const Point& p1, X first)
      : rectangle{p0, p1, p0, p1}, first(first){};

  CanonicalSegment(const Point (&rectangle)[4], X first)
      : rectangle{rectangle[0], rectangle[1], rectangle[2], rectangle[3]},
        first(first){};

  bool one_point() const {
    return rectangle[0].x == rectangle[2].x &&
           rectangle[0].y == rectangle[2].y &&
           rectangle[1].x == rectangle[3].x && rectangle[1].y == rectangle[3].y;
  }

 public:
  CanonicalSegment() = default;

  X get_first_x() const { return first; }

  X get_last_x() const {
    return std::max(rectangle[1].x, rectangle[2].x);
  }

  std::pair<long double, long double> get_intersection() const {
    auto& p0 = rectangle[0];
    auto& p1 = rectangle[1];
    auto& p2 = rectangle[2];
    auto& p3 = rectangle[3];
    auto slope1 = p2 - p0;
    auto slope2 = p3 - p1;

    if (one_point() || slope1 == slope2) return {p0.x, p0.y};

    auto p0p1 = p1 - p0;
    auto a = slope1.dx * slope2.dy - slope1.dy * slope2.dx;
    auto b = (p0p1.dx * slope2.dy - p0p1.dy * slope2.dx) /
             static_cast<long double>(a);
    auto i_x = p0.x + b * slope1.dx;
    auto i_y = p0.y + b * slope1.dy;
    return {i_x, i_y};
  }

  std::pair<long double, SY> get_floating_point_segment(const X& origin) const {
    if (one_point()) return {0, (rectangle[0].y + rectangle[1].y) / 2};

    if constexpr (std::is_integral_v<X> && std::is_integral_v<Y>) {
      auto slope = rectangle[3] - rectangle[1];
      auto intercept_n = slope.dy * (SX(origin) - rectangle[1].x);
      auto intercept_d = slope.dx;
      auto rounding_term =
          ((intercept_n < 0) ^ (intercept_d < 0) ? -1 : +1) * intercept_d / 2;
      auto intercept =
          (intercept_n + rounding_term) / intercept_d + rectangle[1].y;
      return {static_cast<long double>(slope), intercept};
    }

    auto [i_x, i_y] = get_intersection();
    auto [min_slope, max_slope] = get_slope_range();
    auto slope = (min_slope + max_slope) / 2.;
    auto intercept = i_y - (i_x - origin) * slope;
    return {slope, intercept};
  }

  std::pair<long double, long double> get_slope_range() const {
    if (one_point()) return {0, 1};

    auto min_slope = static_cast<long double>(rectangle[2] - rectangle[0]);
    auto max_slope = static_cast<long double>(rectangle[3] - rectangle[1]);
    return {min_slope, max_slope};
  }
};

template <typename Fin, typename Fout>
size_t make_segmentation(size_t n, size_t epsilon, Fin in, Fout out) {
  if (n == 0) return 0;

  using X = typename std::invoke_result_t<Fin, size_t>::first_type;
  using Y = typename std::invoke_result_t<Fin, size_t>::second_type;
  size_t c = 0;
  auto p = in(0);

  OptimalPiecewiseLinearModel<X, Y> opt(epsilon);
  opt.add_point(p.first, p.second);

  for (size_t i = 1; i < n; ++i) {
    auto next_p = in(i);
    if (next_p.first == p.first) continue;
    p = next_p;
    if (!opt.add_point(p.first, p.second)) {
      out(opt.get_segment());
      opt.add_point(p.first, p.second);
      ++c;
    }
  }

  out(opt.get_segment());
  return ++c;
}

// Disabled because this code depends on OpenMP.
#if false
template <typename Fin, typename Fout>
size_t make_segmentation_par(size_t n, size_t epsilon, Fin in, Fout out) {
  auto parallelism =
      std::min(std::min(omp_get_num_procs(), omp_get_max_threads()), 20);
  auto chunk_size = n / parallelism;
  auto c = 0ull;

  if (parallelism == 1 || n < 1ull << 15)
    return make_segmentation(n, epsilon, in, out);

  using X = typename std::invoke_result_t<Fin, size_t>::first_type;
  using Y = typename std::invoke_result_t<Fin, size_t>::second_type;
  using canonical_segment =
      typename OptimalPiecewiseLinearModel<X, Y>::CanonicalSegment;
  std::vector<std::vector<canonical_segment>> results(parallelism);

#pragma omp parallel for reduction(+ : c) num_threads(parallelism)
  for (auto i = 0; i < parallelism; ++i) {
    auto first = i * chunk_size;
    auto last = i == parallelism - 1 ? n : first + chunk_size;
    if (first > 0) {
      for (; first < last; ++first)
        if (in(first).first != in(first - 1).first) break;
      if (first == last) continue;
    }

    auto in_fun = [in, first](auto j) { return in(first + j); };
    auto out_fun = [&results, i](const auto& cs) {
      results[i].emplace_back(cs);
    };
    results[i].reserve(chunk_size / (epsilon > 0 ? epsilon * epsilon : 16));
    c += make_segmentation(last - first, epsilon, in_fun, out_fun);
  }

  for (auto& v : results)
    for (auto& cs : v) out(cs);

  return c;
}
#endif

template <typename RandomIt>
auto make_segmentation(RandomIt first, RandomIt last, size_t epsilon) {
  using key_type = typename RandomIt::value_type;
  using canonical_segment =
      typename OptimalPiecewiseLinearModel<key_type, size_t>::CanonicalSegment;
  using pair_type = typename std::pair<key_type, size_t>;

  size_t n = std::distance(first, last);
  std::vector<canonical_segment> out;
  out.reserve(epsilon > 0 ? n / (epsilon * epsilon) : n / 16);

  auto in_fun = [first](auto i) { return pair_type(first[i], i); };
  auto out_fun = [&out](const auto& cs) { out.push_back(cs); };
  make_segmentation(n, epsilon, in_fun, out_fun);

  return out;
}

}  // namespace pgm
