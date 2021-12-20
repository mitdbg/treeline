from typing import Optional


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def add_delta(self, delta: float) -> "Point":
        return Point(self.x, self.y + delta)

    def __str__(self) -> str:
        return "Point(x={}, y={})".format(self.x, self.y)


class Line:
    def __init__(self, slope: float, intercept: float):
        # y = slope * x + intercept
        self.slope = slope
        self.intercept = intercept

    @classmethod
    def from_slope_and_point(cls, slope: float, point: Point) -> "Line":
        # intercept = y - slope * x
        return Line(slope, point.y - slope * point.x)

    @classmethod
    def from_two_points(cls, x1: Point, x2: Point) -> "Line":
        slope = (x2.y - x1.y) / (x2.x - x1.x)
        return cls.from_slope_and_point(slope, x1)

    def __call__(self, x: float) -> float:
        return self.slope * x + self.intercept

    def intersect(self, other: "Line") -> Point:
        if self.slope == other.slope:
            raise ValueError("Infinite or no intersections.")
        x = (other.intercept - self.intercept) / (self.slope - other.slope)
        return Point(x, self(x))


class Segment:
    def __init__(self, line: Line, start_x: float, end_x: float):
        self.line = line
        self.start_point = Point(start_x, self.line(start_x))
        self.end_point = Point(end_x, self.line(end_x))


class GreedyPLRSegment:
    """
    An implementation of the GreedyPLR algorithm described in

      Qing Xie, Chaoyi Pang, Xiaofang Zhou, Xiangliang Zhang, and Ke Deng. 2014.
      Maximum error-bounded Piecewise Linear Representation for online stream
      approximation. The VLDB Journal 23, 6 (December 2014), 915–937. DOI:
      https://doi.org/10.1007/s00778-014-0355-0

    This class greedily consumes points until it can no longer form a line
    segment that has a maximum error of `delta` relative to each of the provided
    data points.
    """

    def __init__(self, s1: Point, s2: Point, delta: float):
        assert s1.x < s2.x
        assert delta > 0
        self._delta = delta
        s1_bot = s1.add_delta(-delta)
        s1_top = s1.add_delta(delta)
        s2_bot = s2.add_delta(-delta)
        s2_top = s2.add_delta(delta)
        self._slope_bot = GreedyPLRSegment._compute_slope(s1_top, s2_bot)
        self._slope_top = GreedyPLRSegment._compute_slope(s1_bot, s2_top)
        l1 = Line.from_two_points(s1_top, s2_bot)
        l2 = Line.from_two_points(s1_bot, s2_top)
        # Since `delta` is nonzero and `s1.x < s2.x`, one intersection must
        # exist.
        self._s0 = l1.intersect(l2)
        self._start_x = s1.x
        self._last_point = s2

    @staticmethod
    def _compute_slope(x1: Point, x2: Point) -> float:
        return (x2.y - x1.y) / (x2.x - x1.x)

    def offer(self, s: Point) -> Optional[Segment]:
        bottom_line = Line.from_slope_and_point(self._slope_bot, self._s0)
        bottom_pred = bottom_line(s.x)
        if bottom_pred >= s.y - self._delta:
            return self.finish()

        top_line = Line.from_slope_and_point(self._slope_top, self._s0)
        top_pred = top_line(s.x)
        if top_pred <= s.y + self._delta:
            return self.finish()

        x_diff = s.x - self._s0.x
        if abs(s.y - self._slope_bot * x_diff - self._s0.y) > self._delta:
            self._slope_bot = GreedyPLRSegment._compute_slope(
                self._s0, s.add_delta(-self._delta)
            )
        if abs(s.y - self._slope_top * x_diff - self._s0.y) > self._delta:
            self._slope_top = GreedyPLRSegment._compute_slope(
                self._s0, s.add_delta(self._delta)
            )

        self._last_point = s
        return None

    def finish(self) -> Segment:
        return Segment(
            Line.from_slope_and_point(
                (self._slope_bot + self._slope_top) / 2, self._s0
            ),
            self._start_x,
            self._last_point.x,
        )


class GreedyPLR:
    """
    A convenience wrapper around `GreedyPLRSegment` that will return the
    constructed linear segments.

    N.B. This algorithm does not produce connected line segments (i.e.,
    discontinuities across the linear functions are allowed).
    """

    def __init__(self, delta: float):
        self._delta = delta
        self._s1 = None
        self._s2 = None
        self._curr = None

    def offer(self, s: Point) -> Optional[Segment]:
        if self._s1 is None:
            self._s1 = s
            return None
        elif self._s2 is None:
            self._s2 = s
            self._curr = GreedyPLRSegment(self._s1, self._s2, self._delta)
            return None

        res = self._curr.offer(s)
        if res is not None:
            self._s1 = self._curr._last_point
            self._s2 = s
            self._curr = GreedyPLRSegment(self._s1, self._s2, self._delta)
        return res

    def finish(self) -> Optional[Segment]:
        if self._s1 is None:
            return None
        elif self._s2 is None:
            return Line.from_two_points(self._s1, Point(self._s1.x + 1, self._s1.y))

        res = self._curr.finish()
        if res is None:
            return Segment(
                Line.from_two_points(self._s1, self._s2),
                start_x=self._s1.x,
                end_x=self._s2.x,
            )
        else:
            return res
