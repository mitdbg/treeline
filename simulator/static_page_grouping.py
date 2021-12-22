import argparse
import bisect
import collections
import csv
import pathlib

from plr.greedy import GreedyPLR, Point, Segment, Line
from utils.dataset import extract_keys, load_dataset_from_text_file


class SegmentMetadata:
    def __init__(self, boundaries, max_key, model):
        self.boundaries = boundaries
        self.max_key = max_key
        self.model = model

    def to_csv_header(self, out):
        writer = csv.writer(out)
        writer.writerow(
            ["segment_size", "min_key", "max_key", "model_slope", "model_intercept"]
        )

    def to_csv_row(self, out):
        writer = csv.writer(out)
        writer.writerow(
            [
                len(self.boundaries),
                self.boundaries[0],
                self.max_key,
                self.model.line.slope,
                self.model.line.intercept,
            ]
        )


class Stats:
    def __init__(self):
        # The number of segments we can make, by length.
        # The index i corresponds to a segment of length 2^i.
        self.segment_counts = [0] * 5
        self.segments = []

    def to_csv(self, out):
        assert len(self.segments) == sum(self.segment_counts)
        writer = csv.writer(out)
        writer.writerow(["segment_type", "count"])
        writer.writerow(["4k", self.segment_counts[0]])
        writer.writerow(["8k", self.segment_counts[1]])
        writer.writerow(["16k", self.segment_counts[2]])
        writer.writerow(["32k", self.segment_counts[3]])
        writer.writerow(["64k", self.segment_counts[4]])


class Pager:
    def __init__(self, dataset, records_per_page):
        self.dataset = dataset
        self.records_per_page = records_per_page
        self._buffered = []
        self._idx = 0

    def has_next(self):
        return len(self._buffered) > 0 or self._idx < len(self.dataset)

    def put_back(self, boundary):
        self._buffered.append(boundary)

    def next(self):
        if len(self._buffered) > 0:
            return self._buffered.pop()
        else:
            item = self.dataset[self._idx]
            self._idx += self.records_per_page
            return item

    def peek(self):
        if len(self._buffered) > 0:
            return self._buffered[-1]
        else:
            return self.dataset[self._idx]


def run_experiment(dataset, records_per_page):
    dataset.sort()
    stats = Stats()

    # Strategy:
    # - Fill pages first. Record the boundary keys as we go.
    # - Suppose we have the key boundaries: [x_0, x_1), [x_1, x_2), ...
    # - See if we can fit a linear model f(x) where
    #     round(f(x)) = 0  if x_0 <= x < x_1
    #     round(f(x)) = 1  if x_1 <= x < x_2
    #     round(f(x)) = 2  if x_2 <= x < x_3
    #     etc.
    #   - This implies the point constraints: (x_0, 0), (x_1 - 1, 0), (x_1, 1),
    #     (x_2 - 1, 1), etc. with a delta of 0.5 (all errors must be strictly
    #     less than 0.5 so that round(f(x)) produces the desired integer).
    # - We only permit a few segment sizes: 1, 2, 4, 8, 16 (corresponding to 4
    #   KiB, 8 KiB, ..., 64 KiB segments)

    allowed_segment_sizes = [1, 2, 4, 8, 16]
    pager = Pager(dataset, records_per_page)

    while pager.has_next():
        boundaries = collections.deque()
        boundaries.append(pager.next())

        # Attempt to build a segment from here
        base = boundaries[-1]
        segment_size = 1
        prev_x = 0
        plr = GreedyPLR(delta=0.5)
        line = plr.offer(Point(0, 0))
        assert line is None

        while pager.has_next() and segment_size < 16:
            # Use peek instead and actually call next after incrementing the segment size
            next_boundary = pager.peek()
            diff = next_boundary - base
            assert diff > 0
            if diff - 1 != prev_x:
                line = plr.offer(Point(diff - 1, segment_size - 1))
                if line is not None:
                    # Cannot extend the current segment any further with one model
                    break
            prev_x = diff
            line = plr.offer(Point(diff, segment_size))
            if line is not None:
                # Cannot extend the current segment any further with one model
                break

            segment_size += 1
            boundaries.append(pager.next())
            assert boundaries[-1] == next_boundary

        if line is None:
            line = plr.finish()

        if line is None:
            assert len(boundaries) == 1
            assert segment_size == 1
            stats.segment_counts[0] += 1
            stats.segments.append(
                SegmentMetadata(
                    [boundaries[0]],
                    -1,
                    Segment(
                        Line.from_two_points(Point(0, 0), Point(1, 0)),
                        boundaries[0],
                        boundaries[0] + 1,
                    ),
                )
            )
            break

        # Create as large of a segment as possible
        segment_size_idx = bisect.bisect_right(allowed_segment_sizes, segment_size) - 1
        largest_segment_size = allowed_segment_sizes[segment_size_idx]

        # Track the segment size
        stats.segment_counts[segment_size_idx] += 1

        boundaries_in_segment = []
        for _ in range(largest_segment_size):
            boundaries_in_segment.append(boundaries.popleft())

        # Put back any "pages" that were processed but cannot be part of this segment
        while len(boundaries) > 0:
            pager.put_back(boundaries.pop())

        if pager.has_next():
            max_key_in_segment = pager.peek() - 1
        else:
            max_key_in_segment = boundaries_in_segment[-1] + 1

        stats.segments.append(
            SegmentMetadata(
                boundaries_in_segment,
                max_key_in_segment,
                line.adjust_bounds(0, max_key_in_segment - base),
            )
        )

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records_per_page", type=int, default=50)
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    if args.workload_config is None and args.custom_dataset is None:
        print("ERROR: Need to provide either a workload config or a custom dataset.")
        return
    if args.out_dir is None:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(args.out_dir)

    if args.custom_dataset is not None:
        dataset = load_dataset_from_text_file(args.custom_dataset)
    else:
        import ycsbr_py as ycsbr

        workload = ycsbr.PhasedWorkload.from_file(
            args.workload_config,
            set_record_size_bytes=16,
        )
        dataset = extract_keys(workload.get_load_trace())

    res = run_experiment(dataset, args.records_per_page)
    assert len(res.segments) > 0

    with open(out_dir / "summary.csv", "w") as f:
        res.to_csv(f)
    with open(out_dir / "segments.csv", "w") as f:
        res.segments[0].to_csv_header(f)
        for seg in res.segments:
            seg.to_csv_row(f)


if __name__ == "__main__":
    main()
