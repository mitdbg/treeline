import bisect
import math
import statistics
import sortedcontainers
import ycsbr_py as ycsbr

from typing import List, Iterable
from grouping.w_segment_2 import WritablePageSegment2
from grouping.merging import greedy_merge_at, MergeStat


class InsertDB2(ycsbr.DatabaseInterface):
    """
    A version of `InsertDB` meant for use with WritablePageSegment2 (uses a
    different merge strategy).
    """
    def __init__(
        self,
        segments: List[WritablePageSegment2],
        page_goal: int,
        page_delta: int,
        internal_reorg_only: bool,
    ):
        ycsbr.DatabaseInterface.__init__(self)
        self._segments = sortedcontainers.SortedKeyList(
            segments, key=lambda seg: seg.base_key
        )
        self._page_goal = page_goal
        self._page_delta = page_delta
        self._records_per_page_est = statistics.mean(
            map(lambda seg: len(seg.get_all_keys()) / seg.segment_size, self._segments)
        )
        self._internal_reorg_only = internal_reorg_only

        # Statistics
        self.num_inserts = 0
        self.scan_read_counts = [0 for _ in range(16)]
        self.scan_overflow_read_counts = 0
        self.merge_stats: List[MergeStat] = []

    @property
    def segments(self) -> List[WritablePageSegment2]:
        return list(self._segments)

    # DatabaseInterface methods below.

    def initialize_database(self):
        pass

    def shutdown_database(self):
        pass

    def bulk_load(self, load):
        pass

    def insert(self, key, val):
        self.num_inserts += 1

        seg_idx = self._segment_for_key(key)
        seg = self._segments[seg_idx]
        succeeded = seg.insert(key)
        if succeeded:
            return True

        # Segment (incl. overflow) is full. Trigger a reorg.
        res = greedy_merge_at(
            self._segments,
            seg_idx,
            self._page_goal,
            self._page_delta,
            single_only=self._internal_reorg_only,
        )
        assert res.remove_count > 0
        assert len(res.new_segments) > 0
        removed_segments = []
        for _ in range(res.remove_count):
            removed_segments.append(self._segments.pop(res.remove_start_idx))
        for s in res.new_segments:
            self._segments.add(s)
        self.merge_stats.append(
            MergeStat(
                in_segments=res.remove_count,
                in_pages=self._page_count(removed_segments),
                out_segments=len(res.new_segments),
                out_pages=self._page_count(res.new_segments),
            )
        )

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
        # Scan algorithm:
        # - Key space is split into segments, each segment has a fixed size
        #   plus an overflow.
        # - When scanning a segment we must read the entire overflow (for merging).
        # - We use page fill statistics to estimate how much of the segment to read.
        #
        # In this simulation we measure how much I/O we do and how large each I/O is.
        #
        # N.B.: All YCSB scans are forward scans.
        # 1. Find correct segment.
        # 2. Map to correct page.
        # 3. Estimate how much of the segment to read.
        # 4. Scan forward, estimating how much of the next segment to read based
        #    on page fullness statistics.
        records_left = amount

        # First segment handling.
        seg_id = self._segment_for_key(start)
        seg = self._segments[seg_id]
        if seg.model is not None:
            page_raw = seg.model.line(start - seg.base_key)
        else:
            page_raw = 0
        page_idx = int(page_raw)
        page_idx = max(0, min(page_idx, len(seg.pages) - 1))

        # Estimate how much of the segment to read.
        key_pos_est = page_raw - page_idx
        recs_on_first_page_est = int(key_pos_est * self._records_per_page_est)
        pages_left_in_first_segment = seg.segment_size - page_idx - 1
        pages_left_to_read_est = math.ceil(
            (records_left - recs_on_first_page_est) / self._records_per_page_est
        )
        pages_to_read_in_first_seg = 1 + min(
            pages_left_in_first_segment, pages_left_to_read_est
        )
        pages_to_read_in_first_seg = min(16, max(1, pages_to_read_in_first_seg))

        # Record the I/O in the first segment.
        self.scan_read_counts[pages_to_read_in_first_seg - 1] += 1

        # If there is an overflow, we need to read all of it in.
        if seg.has_overflow:
            self.scan_overflow_read_counts += seg.overflow_page_count

        record_source = _ScanMerger(seg.overflow)

        # "Scan" records on the first page.
        first_page = seg.pages[page_idx]
        start_on_page = bisect.bisect_left(first_page, start)
        record_source.add_page_keys(first_page[start_on_page:])
        records_left -= record_source.advance_next_n_records(n=records_left)

        # Process additional pages in the first segment
        page_idx += 1
        pages_actually_read_in_first_segment = 1
        while records_left > 0 and page_idx < seg.segment_size:
            record_source.add_page_keys(seg.pages[page_idx])
            records_left -= record_source.advance_next_n_records(n=records_left)
            page_idx += 1
            pages_actually_read_in_first_segment += 1

        # Record any differences in our predicted vs. actual page read count
        if pages_actually_read_in_first_segment > pages_to_read_in_first_seg:
            diff = pages_actually_read_in_first_segment - pages_to_read_in_first_seg
            # 4 KiB (single page) reads
            self.scan_read_counts[0] += diff

        # Any leftover overflow records should be counted.
        if records_left > 0:
            records_left -= record_source.num_overflow_left()

        # Handle remaining segments.
        seg_id += 1
        while records_left > 0 and seg_id < len(self._segments):
            seg = self._segments[seg_id]
            pages_left_to_read_est = math.ceil(
                records_left / self._records_per_page_est
            )
            pages_to_read_in_seg_est = min(pages_left_to_read_est, seg.segment_size)
            # Record the I/O.
            self.scan_read_counts[pages_to_read_in_seg_est - 1] += 1

            if seg.has_overflow:
                self.scan_overflow_read_counts += seg.overflow_page_count
            record_source = _ScanMerger(seg.overflow)

            page_idx = 0
            while records_left > 0 and page_idx < seg.segment_size:
                record_source.add_page_keys(seg.pages[page_idx])
                records_left -= record_source.advance_next_n_records(n=records_left)
                page_idx += 1
            pages_actually_read = page_idx

            # Record underestimates
            if pages_actually_read > pages_to_read_in_seg_est:
                diff = pages_actually_read - pages_to_read_in_seg_est
                self.scan_read_counts[0] += diff

            # Any leftover overflow records should be counted.
            if records_left > 0:
                records_left -= record_source.num_overflow_left()

            seg_id += 1

        return []

    def _segment_for_key(self, key):
        idx = self._segments.bisect_key_right(key)
        if idx > 0:
            idx -= 1
        return idx

    @staticmethod
    def _page_count(segments: Iterable[WritablePageSegment2]) -> int:
        return sum(map(lambda seg: seg.segment_size, segments))


class _ScanMerger:
    def __init__(self, overflow: List[int]):
        self._overflow = overflow
        self._overflow.sort()
        self._page_keys = []

        self._overflow_idx = 0
        self._page_key_idx = 0

    def add_page_keys(self, sorted_keys: List[int]):
        self._page_keys.extend(sorted_keys)

    def advance_next_n_records(self, n: int) -> int:
        """
        Simulate draining the next `n` smallest keys, prioritizing the pages
        first.
        """
        num_advanced = 0
        while (
            num_advanced < n
            and self._overflow_idx < len(self._overflow)
            and self._page_key_idx < len(self._page_keys)
        ):
            if (
                self._page_keys[self._page_key_idx]
                <= self._overflow[self._overflow_idx]
            ):
                self._page_key_idx += 1
            else:
                self._overflow_idx += 1
            num_advanced += 1

        while num_advanced < n and self._page_key_idx < len(self._page_keys):
            self._page_key_idx += 1
            num_advanced += 1

        return num_advanced

    def num_overflow_left(self) -> int:
        return max(0, len(self._overflow) - self._overflow_idx)
