import bisect
import ycsbr_py as ycsbr
from typing import List
from grouping.w_segment import WritablePageSegment


class InsertDB(ycsbr.DatabaseInterface):
    def __init__(
        self, segments: List[WritablePageSegment], page_goal: int, page_delta: int
    ):
        ycsbr.DatabaseInterface.__init__(self)
        self._segments = segments
        self._page_boundaries = []
        self._generate_page_boundaries()
        self._page_goal = page_goal
        self._page_delta = page_delta

        # Statistics
        self._num_inserts = 0
        self._num_reorgs = 0

    @property
    def segments(self) -> List[WritablePageSegment]:
        return self._segments

    # DatabaseInterface methods below.

    def initialize_database(self):
        pass

    def shutdown_database(self):
        pass

    def bulk_load(self, load):
        pass

    def insert(self, key, val):
        self._num_inserts += 1

        seg_idx = self._segment_for_key(key)
        seg = self._segments[seg_idx]
        succeeded = seg.insert(key)
        if succeeded:
            return True

        self._num_reorgs += 1
        new_segs = seg.reorg(page_goal=self._page_goal, page_delta=self._page_delta)
        assert len(new_segs) >= 1
        # Delete the old segment.
        self._segments[seg_idx] = self._segments[-1]
        self._segments.pop()
        self._segments.extend(new_segs)
        self._generate_page_boundaries()

        # Redo the insert. It must succeed.
        seg_idx = self._segment_for_key(key)
        seg = self._segments[seg_idx]
        succeeded = seg.insert(key)
        assert succeeded
        return True

    def update(self, key, val):
        return True

    def read(self, key):
        return None

    def scan(self, start, amount):
        return []

    def _generate_page_boundaries(self):
        self._segments.sort(key=lambda seg: seg.base_key)
        self._page_boundaries = list(map(lambda seg: seg.base_key, self._segments))

    def _segment_for_key(self, key):
        idx = bisect.bisect_right(self._page_boundaries, key)
        if idx > 0:
            idx -= 1
        return idx
