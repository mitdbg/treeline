import ycsbr_py as ycsbr
from utils.dataset import process_dataset
from utils.lru import LRUCache


class RecordInCache:
    def __init__(self, dirty):
        self.dirty = dirty


class LRUCacheDB(ycsbr.DatabaseInterface):
    Name = "lru"

    def __init__(self, dataset, keys_per_page, cache_capacity, admit_read_pages=False):
        ycsbr.DatabaseInterface.__init__(self)
        page_mapper, page_data = process_dataset(dataset, keys_per_page)
        self._page_mapper = page_mapper
        self._page_data = page_data
        self._cache = LRUCache(cache_capacity)
        self._records_in_cache = {}
        self._admit_read_pages = admit_read_pages
        self._read_ios = 0
        self._write_ios = 0

    @property
    def read_ios(self):
        return self._read_ios

    @property
    def write_ios(self):
        return self._write_ios

    @property
    def hit_rate(self):
        return self._cache.hit_rate

    def _handle_evicted(self, evicted):
        if evicted is None:
            return

        # Write out all dirty keys in the same page.
        key, _ = evicted
        page = self._page_mapper(key)
        for page_key in self._page_data[page]:
            if page_key not in self._records_in_cache:
                continue
            needs_write = needs_write or self._records_in_cache[page_key].dirty
            self._records_in_cache[page_key].dirty = False

        del self._records_in_cache[key]
        if needs_write:
            self._write_ios += 1

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
        if self._cache.lookup(key):
            self._records_in_cache[key].dirty = True
            return
        evicted = self._cache.add(key, 0)
        self._records_in_cache[key] = RecordInCache(dirty=True)
        self._handle_evicted(evicted)
        return True

    def read(self, key):
        if self._cache.lookup(key):
            return
        self._read_ios += 1
        if self._admit_read_pages:
            for page_key in self._page_data[self._page_mapper(key)]:
                evicted = self._cache.add(page_key, 0)
                self._records_in_cache[page_key] = RecordInCache(dirty=False)
                self._handle_evicted(evicted)
        else:
            evicted = self._cache.add(key, 0)
            self._records_in_cache[key] = RecordInCache(dirty=False)
            self._handle_evicted(evicted)

    def scan(self, start, amount):
        return []
