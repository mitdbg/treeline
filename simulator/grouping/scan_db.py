import sortedcontainers
import math
import ycsbr_py as ycsbr
from typing import List


class ScanDB(ycsbr.DatabaseInterface):
    def __init__(
        self,
        dataset: List[int],
        keys_per_page_goal: int,
        max_keys_per_page: int,
        reorg_at_length: int,
    ):
        ycsbr.DatabaseInterface.__init__(self)
        self._scanned_pages = 0
        self._reorg_at_length = reorg_at_length
        self._keys_per_page_goal = keys_per_page_goal
        self.num_inserts = 0
        self.num_insert_triggered_reorgs = 0

        dataset.sort()
        curr_page = []
        all_pages = []
        for key in dataset:
            curr_page.append(key)
            if len(curr_page) >= keys_per_page_goal:
                all_pages.append(_PageChain(curr_page, max_keys_per_page))
                curr_page = []
        if len(curr_page) > 0:
            all_pages.append(_PageChain(curr_page, max_keys_per_page))

        self._page_chains = sortedcontainers.SortedKeyList(
            all_pages, key=lambda chain: chain.base_key
        )

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
        self.num_inserts += 1
        chain_id = self._chain_for_key(key)
        chain = self._page_chains[chain_id]
        chain.add(key)
        if chain.chain_length >= self._reorg_at_length:
            new = chain.flatten(self._keys_per_page_goal)
            assert len(new) >= 1
            self._page_chains.remove(chain)
            for c in new:
                self._page_chains.add(c)
            self.num_insert_triggered_reorgs += 1
        return True

    def update(self, key, val):
        return True

    def read(self, key):
        return None

    def scan(self, start, amount):
        records_left = amount

        # Handling the first page.
        start_page_id = self._chain_for_key(start)
        start_chain = self._page_chains[start_page_id]
        self._scanned_pages += start_chain.chain_length
        records_left -= start_chain.keys_forward_scanned(start)

        # Scan forward until we read enough records.
        curr_idx = start_page_id + 1
        while records_left > 0 and curr_idx < len(self._page_chains):
            chain = self._page_chains[curr_idx]
            self._scanned_pages += chain.chain_length
            records_left -= chain.num_keys
            curr_idx += 1

        return []

    def _chain_for_key(self, key):
        idx = self._page_chains.bisect_key_right(key)
        if idx > 0:
            idx -= 1
        return idx


class _PageChain:
    def __init__(self, keys: List[int], max_keys_per_page: int):
        assert len(keys) > 0
        self.keys = sortedcontainers.SortedList(keys)
        self.base_key = self.keys[0]
        self._max_keys_per_page = max_keys_per_page

    @property
    def chain_length(self):
        return math.ceil(len(self.keys) / self._max_keys_per_page)

    @property
    def num_keys(self):
        return len(self.keys)

    def add(self, key):
        self.keys.add(key)

    def flatten(self, keys_per_page_goal: int) -> "List[_PageChain]":
        num_pages = math.ceil(len(self.keys) / keys_per_page_goal)
        keys_per_page = len(self.keys) // num_pages
        remainder = len(self.keys) % num_pages
        # Extra key should fit on the page (remainder)
        assert keys_per_page + 1 <= self._max_keys_per_page

        flat_chains = []
        next_page = []
        iterator = iter(self.keys)
        try:
            while True:
                next_page.append(next(iterator))
                if len(next_page) >= keys_per_page:
                    if remainder > 0:
                        next_page.append(next(iterator))
                        remainder -= 1
                    flat_chains.append(_PageChain(next_page, self._max_keys_per_page))
                    next_page.clear()
        except StopIteration:
            pass
        if len(next_page) > 0:
            flat_chains.append(_PageChain(next_page, self._max_keys_per_page))

        return flat_chains

    def keys_forward_scanned(self, key):
        return len(self.keys) - self.keys.bisect_left(key)
