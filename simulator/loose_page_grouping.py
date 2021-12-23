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
            # Could not model enough keys to fit into one segment. So we just
            # fill the page completely.
            assert len(keys_in_segment) < allowed_records_in_segments[0]
            while (
                it.has_next() and len(keys_in_segment) < allowed_records_in_segments[0]
            ):
                keys_in_segment.append(it.next())
            segments.append(PageSegment(keys=keys_in_segment, model=None, page_count=1))
            continue

        segment_page_count = SEGMENT_PAGE_COUNTS[segment_size_idx]
        records_in_segment = allowed_records_in_segments[segment_size_idx]

        if segment_page_count == 1:
            # Special case where we can fill 1 page, but not 2. We do not need a
            # model for 1 page.
            num_extra_keys = len(keys_in_segment) - records_in_segment
            assert num_extra_keys >= 0
            it.rollback(num_extra_keys)
            segments.append(
                PageSegment(
                    keys=keys_in_segment[:records_in_segment], model=None, page_count=1
                )
            )
            continue

        # We can index at least 2 pages. Use the derived model to figure out how
        # many records that corresponds to, and then create the segment.

        cutoff_idx = len(keys_in_segment)
        while (
            cutoff_idx > 0
            and int(line.line(keys_in_segment[cutoff_idx - 1] - base))
            >= records_in_segment
        ):
            cutoff_idx -= 1
        assert (
            int(line.line(keys_in_segment[cutoff_idx - 1] - base)) < records_in_segment
        )

        num_extra_keys = len(keys_in_segment) - cutoff_idx
        assert num_extra_keys >= 0
        it.rollback(num_extra_keys)

        # Put records into pages
        bounded_model = line.adjust_bounds(0, cutoff_idx)
        bounded_model.scale_in_place(goal)
        segments.append(
            PageSegment(
                keys=keys_in_segment[:cutoff_idx],
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


def validate_segments(segments, goal, delta):
    min_in_page = goal - delta
    max_in_page = goal + delta

    def validate_size(num_keys, segment, page):
        if num_keys > max_in_page:
            print(
                "Validation Error: Segment {} page {} has too many keys ({}, expected at most {})".format(
                    segment, page, num_keys, max_in_page
                )
            )
        if num_keys < min_in_page:
            print(
                "Validation Error: Segment {} page {} has too few keys ({}, expected at least {})".format(
                    segment, page, num_keys, min_in_page
                )
            )

    for segment_id, segment in enumerate(segments):
        pages = [[] for _ in range(segment.page_count)]
        if segment.model is None or segment.page_count == 1:
            assert segment.page_count == 1
            validate_size(len(segment.keys), segment_id, page=0)
            continue

        for key in segment.keys:
            page_id_raw = segment.model.line(key - segment.base)
            page_id = int(page_id_raw)
            if page_id < 0 or page_id >= len(pages):
                print(
                    "Validation Error: Segment {} model produced an out of bound page for key {} (page_id_raw: {})".format(
                        segment_id, key, page_id_raw
                    )
                )
            else:
                pages[page_id].append(key)

        for page_id, page in enumerate(pages):
            validate_size(len(page), segment_id, page_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--validate", action="store_true")
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

    if args.validate:
        validate_segments(
            segments, goal=args.records_per_page_goal, delta=args.records_per_page_delta
        )


if __name__ == "__main__":
    main()
