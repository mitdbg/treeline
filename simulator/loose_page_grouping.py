import argparse
import bisect
import csv
import pathlib

from plr.greedy import GreedyPLR, Point
from utils.dataset import extract_keys, load_dataset_from_text_file


SEGMENT_PAGE_COUNTS = [1, 2, 4, 8, 16]
SEGMENT_PAGE_COUNTS_TO_INDEX = {
    count: idx for idx, count in enumerate(SEGMENT_PAGE_COUNTS)
}


class ListIterator:
    def __init__(self, dataset):
        self.dataset = dataset
        self.idx = 0

    def has_next(self):
        return self.idx < len(self.dataset)

    def next(self):
        item = self.dataset[self.idx]
        self.idx += 1
        return item

    def peek(self):
        return self.dataset[self.idx]

    def rollback(self, num):
        assert num <= self.idx
        self.idx -= num


class PageSegment:
    def __init__(self, keys, model, page_count):
        self.keys = keys
        self.base = self.keys[0]
        self.model = model
        self.page_count = page_count


def build_segments(dataset, goal, delta):
    allowed_records_in_segments = list(map(lambda s: s * goal, SEGMENT_PAGE_COUNTS))
    max_per_segment = allowed_records_in_segments[-1]

    dataset.sort()
    segments = []
    it = ListIterator(dataset)

    while it.has_next():
        base = it.next()
        plr = GreedyPLR(delta=delta)
        line = plr.offer(Point(0, 0))
        records_considered = 1
        keys_in_segment = [base]
        assert line is None

        # Attempt to build as large of a segment as possible
        while it.has_next() and records_considered < max_per_segment:
            next_key = it.peek()
            diff = next_key - base
            # N.B. There are no duplicates allowed. So the x-coord of the point
            # being offered is always different from the previous point.
            line = plr.offer(Point(diff, records_considered))
            if line is not None:
                # Cannot extend the segment further with the current point
                break
            records_considered += 1
            keys_in_segment.append(next_key)
            it.next()

        if line is None:
            line = plr.finish()

            if line is None:
                assert records_considered == 1 and len(keys_in_segment) == 1
                segments.append(
                    PageSegment(keys=keys_in_segment, model=None, page_count=1)
                )
                continue

        # Create as large of a segment as possible
        segment_size_idx = (
            bisect.bisect_right(allowed_records_in_segments, records_considered) - 1
        )
        if segment_size_idx < 0:
            segment_size_idx = 0
        segment_page_count = SEGMENT_PAGE_COUNTS[segment_size_idx]
        records_in_segment = allowed_records_in_segments[segment_size_idx]

        actual_records_in_segment = min(len(keys_in_segment), records_in_segment)
        num_extra_keys = len(keys_in_segment) - actual_records_in_segment
        it.rollback(num_extra_keys)

        # Put records into pages
        bounded_model = line.adjust_bounds(0, actual_records_in_segment - 1)
        bounded_model.scale_in_place(goal)
        segments.append(
            PageSegment(
                keys=keys_in_segment[:actual_records_in_segment],
                model=bounded_model,
                page_count=segment_page_count,
            )
        )

    return segments


def write_summary(segments, out_dir):
    counts = [0 for _ in range(len(SEGMENT_PAGE_COUNTS))]
    for segment in segments:
        counts[SEGMENT_PAGE_COUNTS_TO_INDEX[segment.page_count]] += 1

    with open(out_dir / "summary.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_type", "count"])
        writer.writerow(["4k", counts[0]])
        writer.writerow(["8k", counts[1]])
        writer.writerow(["16k", counts[2]])
        writer.writerow(["32k", counts[3]])
        writer.writerow(["64k", counts[4]])

    return counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
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

    segments = build_segments(
        dataset, goal=args.records_per_page_goal, delta=args.records_per_page_delta
    )

    write_summary(segments, out_dir)


if __name__ == "__main__":
    main()
