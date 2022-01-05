import bisect
import ycsbr_py as ycsbr
from typing import List


class ScanDB(ycsbr.DatabaseInterface):
    def __init__(self, dataset: List[int], keys_per_page: int):
        ycsbr.DatabaseInterface.__init__(self)
        self._scanned_pages = 0
        self._page_boundaries = []
        self._pages: List[List[int]] = []

        def page_mapper(key):
            idx = bisect.bisect_right(self._page_boundaries, key)
            if idx > 0:
                idx -= 1
            return idx

        self._page_mapper = page_mapper
    
        dataset.sort()
        curr_page = []
        for key in dataset:
            if len(curr_page) == 0:
                self._page_boundaries.append(key)
            curr_page.append(key)
            if len(curr_page) >= keys_per_page:
                self._pages.append(curr_page)
                curr_page = []
        if len(curr_page) > 0:
            self._pages.append(curr_page)

    @property
    def scanned_pages(self):
        return self._scanned_pages

    # DatabaseInterface methods below.

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
        records_left = amount

        # Handling the first page.
        start_page_id = self._page_mapper(start)
        start_page = self._pages[start_page_id]
        self._scanned_pages += 1
        start_scanning_idx = bisect.bisect_left(start_page, start)
        records_left -= len(start_page) - start_scanning_idx

        # Scan forward until we read enough records.
        curr_idx = start_page_id + 1
        while records_left > 0 and curr_idx < len(self._pages):
            self._scanned_pages += 1
            records_left -= len(self._pages[curr_idx])
            curr_idx += 1

        return []
