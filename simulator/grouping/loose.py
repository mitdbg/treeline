import bisect
from typing import List
from plr.greedy import GreedyPLR, Point
from grouping.ro_segment import ReadOnlyPageSegment

# Represents numbers of 4 KiB pages
SEGMENT_PAGE_COUNTS = [1, 2, 4, 8, 16]
SEGMENT_PAGE_COUNTS_TO_INDEX = {
    count: idx for idx, count in enumerate(SEGMENT_PAGE_COUNTS)
}


class _ListIterator:
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


def build_segments(
    dataset: List[int], goal: int, delta: int, skip_one_page_model: bool = False
) -> List[ReadOnlyPageSegment]:
    allowed_records_in_segments = list(map(lambda s: s * goal, SEGMENT_PAGE_COUNTS))
    max_per_segment = allowed_records_in_segments[-1]

    dataset.sort()
    segments = []
    it = _ListIterator(dataset)

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
                    ReadOnlyPageSegment(keys=keys_in_segment, model=None, page_count=1)
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
            segments.append(
                ReadOnlyPageSegment(keys=keys_in_segment, model=None, page_count=1)
            )
            continue

        segment_page_count = SEGMENT_PAGE_COUNTS[segment_size_idx]
        records_in_segment = allowed_records_in_segments[segment_size_idx]

        if skip_one_page_model and segment_page_count == 1:
            # Special case where we can fill 1 page, but not 2. We do not need a
            # model for 1 page. The caller has requested skipping storing models
            # for 1 page segments.
            num_extra_keys = len(keys_in_segment) - records_in_segment
            assert num_extra_keys >= 0
            it.rollback(num_extra_keys)
            segments.append(
                ReadOnlyPageSegment(
                    keys=keys_in_segment[:records_in_segment], model=None, page_count=1
                )
            )
            continue

        # Use the derived model to figure out how many records that corresponds
        # to, and then create the segment.

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
            ReadOnlyPageSegment(
                keys=keys_in_segment[:cutoff_idx],
                model=bounded_model,
                page_count=segment_page_count,
            )
        )

    return segments


def validate_segments(
    segments: List[ReadOnlyPageSegment], goal: int, delta: int
) -> None:
    min_in_page = goal - delta
    max_in_page = goal + delta

    def validate_size(num_keys: int, segment: int, page: int):
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
        for page_id, page in enumerate(segment.pages):
            validate_size(len(page), segment_id, page_id)


def build_single_segments(
    dataset: List[int], goal: int, delta: int
) -> List[ReadOnlyPageSegment]:
    """
    Breaks up the dataset into single-page segments that include a model if
    possible.
    """
    max_per_segment = goal

    dataset.sort()
    segments = []
    it = _ListIterator(dataset)

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
                    ReadOnlyPageSegment(keys=keys_in_segment, model=None, page_count=1)
                )
                continue

        if len(keys_in_segment) < max_per_segment:
            # Could not model enough keys to fit into one segment. So we just
            # fill the page completely.
            while (
                it.has_next() and len(keys_in_segment) < max_per_segment
            ):
                keys_in_segment.append(it.next())
            segments.append(
                ReadOnlyPageSegment(keys=keys_in_segment, model=None, page_count=1)
            )
            continue

        segment_page_count = 1
        records_in_segment = max_per_segment

        # Use the derived model to figure out how many records that corresponds
        # to, and then create the segment.

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
            ReadOnlyPageSegment(
                keys=keys_in_segment[:cutoff_idx],
                model=bounded_model,
                page_count=segment_page_count,
            )
        )

    return segments
