import argparse
import bisect
import collections
import csv
import sys

from plr.greedy import GreedyPLR, Point


class Stats:
    def __init__(self):
        # The number of segments we can make, by length.
        # The index i corresponds to a segment of length 2^i.
        self.segment_counts = [0] * 5

    def to_csv(self, out):
        writer = csv.writer(out)
        writer.writerow(["4k", "8k", "16k", "32k", "64k"])
        writer.writerow(self.segment_counts)


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
        line = None

        while pager.has_next() and segment_size < 16:
            boundaries.append(pager.next())
            key = boundaries[-1]
            diff = key - base
            assert diff > 0
            if diff - 1 != prev_x:
                line = plr.offer(Point(diff - 1, segment_size - 1))
                if line is not None:
                    break
            line = plr.offer(Point(diff, segment_size))
            if line is not None:
                break
            segment_size += 1

        if line is None:
            line = plr.finish()

        if line is None:
            # Just one page left
            assert len(boundaries) == 1
            assert not pager.has_next()
            assert segment_size == 1
            stats.segment_counts[0] += 1
            break

        # Create as large of a segment as possible
        segment_size_idx = bisect.bisect_right(allowed_segment_sizes, segment_size) - 1
        largest_segment_size = allowed_segment_sizes[segment_size_idx]

        # Track the segment size
        stats.segment_counts[segment_size_idx] += 1

        # Put back any "pages" that were processed but cannot be part of this segment
        for _ in range(largest_segment_size):
            boundaries.popleft()
        while len(boundaries) > 0:
            pager.put_back(boundaries.pop())

    return stats


def load_dataset(filepath):
    # N.B. Dataset needs to be able to fit into memory for this simulation.
    with open(filepath) as f:
        return [int(line) for line in f.readlines()]


def extract_keys(ycsbr_dataset):
    keys = []
    for i in range(len(ycsbr_dataset)):
        keys.append(ycsbr_dataset.get_key_at(i))
    return keys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records_per_page", type=int, default=50)
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    args = parser.parse_args()

    if args.workload_config is None and args.custom_dataset is None:
        print("ERROR: Need to provide either a workload config or a custom dataset.")
        return

    if args.custom_dataset is not None:
        dataset = load_dataset(args.custom_dataset)
    else:
        import ycsbr_py as ycsbr
        workload = ycsbr.PhasedWorkload.from_file(
            args.workload_config,
            set_record_size_bytes=16,
        )
        dataset = extract_keys(workload.get_load_trace())

    res = run_experiment(dataset, args.records_per_page)
    res.to_csv(sys.stdout)


if __name__ == "__main__":
    main()
