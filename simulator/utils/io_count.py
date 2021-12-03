import bisect
import ycsbr_py as ycsbr

from utils.lru import LRUCache


class PageStatus:
    def __init__(self):
        self.dirty = False


class IOCounter(ycsbr.DatabaseInterface):
    def __init__(self, page_mapper, cache_capacity):
        ycsbr.DatabaseInterface.__init__(self)
        self._page_mapper = page_mapper
        self._key_access_freqs = {}
        self._cache = LRUCache(cache_capacity)
        self._read_ios = 0
        self._write_ios = 0

    @staticmethod
    def get_key_order_page_mapper(dataset, keys_per_page):
        page_boundaries = []

        def page_mapper(key):
            return bisect.bisect_left(page_boundaries, key)
    
        dataset.sort()
        count = 0
        for key in dataset:
            if count == 0:
                page_boundaries.append(key)
            count += 1
            if count >= keys_per_page:
                count = 0

        return page_mapper

    @property
    def read_ios(self):
        return self._read_ios

    @property
    def write_ios(self):
        return self._write_ios

    @property
    def key_access_freqs(self):
        return self._key_access_freqs

    def _update_key_access(self, key):
        if key not in self._key_access_freqs:
            self._key_access_freqs[key] = 1
        else:
            self._key_access_freqs[key] += 1

    # DatabaseInterface methods below.

    def initialize_database(self):
        pass

    def shutdown_database(self):
        # Count I/Os needed to write out all dirty pages
        for _, page_status in self._cache.items():
            if not page_status.dirty:
                continue
            self._write_ios += 1
        self._cache.clear()

    def bulk_load(self, load):
        pass

    def insert(self, key, val):
        return True

    def update(self, key, val):
        self._update_key_access(key)
        page_id = self._page_mapper(key)
        page_status = self._cache.lookup(page_id)
        if page_status is not None:
            # Page is cached. Just mark it dirty.
            page_status.dirty = True
            return True

        # Read page from disk
        self._read_ios += 1
        page_status = PageStatus()
        page_status.dirty = True
        evicted = self._cache.add(page_id, page_status)

        if evicted is not None and evicted[1].dirty:
            # Requires a dirty page write out
            self._write_ios += 1

        return True

    def read(self, key):
        self._update_key_access(key)
        page_id = self._page_mapper(key)
        page_status = self._cache.lookup(page_id)
        if page_status is not None:
            # No I/O needed
            return None
        
        # Read page from disk
        self._read_ios += 1
        page_status = PageStatus()
        page_status.dirty = False
        evicted = self._cache.add(page_id, page_status)

        if evicted is not None and evicted[1].dirty:
            # Requires a dirty page write out
            self._write_ios += 1

        return None

    def scan(self, start, amount):
        return []
