import bisect
import math
import statistics
import ycsbr_py as ycsbr


class SegmentedDB(ycsbr.DatabaseInterface):
    def __init__(self, segments):
        ycsbr.DatabaseInterface.__init__(self)
        self._segments = segments
        self._segments.sort(key=lambda seg: seg.base)
        self._boundaries = list(map(lambda seg: seg.base, self._segments))
        # Segment size (in # of pages) -> I/O counts
        self._read_counts = [0 for _ in range(16)]
        self._records_per_page_est = statistics.mean(
            map(lambda seg: len(seg.keys) / seg.page_count, self._segments)
        )

    @property
    def read_counts(self):
        return self._read_counts

    def initialize_database(self):
        pass

    def shutdown_database(self):
        pass

    def bulk_load(self, load):
        pass

    def insert(self, key, val):
        return True

    def update(self, key, val):
        return True

    def read(self, key):
        return None

    def scan(self, start, amount):
        # N.B.: All YCSB scans are forward scans.
        # 1. Find correct segment.
        # 2. Map to correct page.
        # 3. Estimate how much of the segment to read.
        # 4. Scan forward, estimating how much of the next segment to read based
        #    on page fullness statistics.
        records_left = amount

        # First segment handling.
        seg_id = self._find_segment_idx_for(start)
        seg = self._segments[seg_id]
        if seg.model is not None:
            page_raw = seg.model.line(start - seg.base)
        else:
            page_raw = 0
        page_idx = int(page_raw)

        # Estimate how much of the segment to read.
        key_pos_est = page_raw - page_idx
        recs_on_first_page_est = int(key_pos_est * self._records_per_page_est)
        pages_left_in_first_segment = seg.page_count - page_idx - 1
        pages_left_to_read_est = math.ceil(
            (records_left - recs_on_first_page_est) / self._records_per_page_est
        )
        pages_to_read_in_first_seg = 1 + min(
            pages_left_in_first_segment, pages_left_to_read_est
        )

        # Record the I/O in the first segment.
        self._read_counts[pages_to_read_in_first_seg - 1] += 1

        # "Scan" records on the first page.
        first_page = seg.pages[page_idx]
        start_on_page = bisect.bisect_left(first_page, start)
        records_left -= len(first_page) - start_on_page

        # Process additional pages in the first segment
        page_idx += 1
        pages_actually_read_in_first_segment = 1
        while records_left > 0 and page_idx < seg.page_count:
            records_left -= len(seg.pages[page_idx])
            page_idx += 1
            pages_actually_read_in_first_segment += 1

        # Record any differences in our predicted vs. actual page read count
        if pages_actually_read_in_first_segment > pages_to_read_in_first_seg:
            diff = pages_actually_read_in_first_segment - pages_to_read_in_first_seg
            # 4 KiB (single page) reads
            self._read_counts[0] += diff

        # Handle remaining segments.
        seg_id += 1
        while records_left > 0 and seg_id < len(self._segments):
            seg = self._segments[seg_id]
            pages_left_to_read_est = math.ceil(records_left / self._records_per_page_est)
            pages_to_read_in_seg_est = min(pages_left_to_read_est, seg.page_count)
            # Record the I/O.
            self._read_counts[pages_to_read_in_seg_est - 1] += 1

            page_idx = 0
            while records_left > 0 and page_idx < seg.page_count:
                records_left -= len(seg.pages[page_idx])
                page_idx += 1
            pages_actually_read = page_idx

            # Record underestimates
            if pages_actually_read > pages_to_read_in_seg_est:
                diff = pages_actually_read - pages_to_read_in_seg_est
                self._read_counts[0] += diff

            seg_id += 1

        return []

    def _find_segment_idx_for(self, key):
        idx = bisect.bisect_right(self._boundaries, key)
        if idx > 0:
            idx -= 1
        return idx
