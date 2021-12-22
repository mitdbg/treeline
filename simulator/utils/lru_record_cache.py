import ycsbr_py as ycsbr
from utils.dataset import process_dataset
from utils.lru import LRUCache


class RecordInCache:
    def __init__(self, dirty):
        self.dirty = dirty


class LRUCacheDB(ycsbr.DatabaseInterface):
    Name = "lru"

    def __init__(self, dataset, keys_per_page, cache_capacity, admit_read_pages, log_writes_period):
        ycsbr.DatabaseInterface.__init__(self)
        page_mapper, page_data = process_dataset(dataset, keys_per_page)
        self._page_mapper = page_mapper
        self._page_data = page_data
        self._cache = LRUCache(cache_capacity)
        self._records_in_cache = {}
        self._admit_read_pages = admit_read_pages
        self._read_ios = 0
        self._write_ios = 0

        self._logged_record_count = 0
        self._log_writes_period = log_writes_period
        self._write_count = 0

    @property
    def read_ios(self):
        return self._read_ios

    @property
    def write_ios(self):
        return self._write_ios

    @property
    def hit_rate(self):
        return self._cache.hit_rate

    @property
    def logged_record_count(self):
        return self._logged_record_count

    def log_writes(self):
        for record in self._records_in_cache.values():
            if not record.dirty:
                continue
            self._logged_record_count += 1

    def _handle_evicted(self, evicted):
        if evicted is None:
            return

        # Write out all dirty keys in the same page.
        key, _ = evicted
        page = self._page_mapper(key)
        needs_write = False
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
        else:
            evicted = self._cache.add(key, 0)
            self._records_in_cache[key] = RecordInCache(dirty=True)
            self._handle_evicted(evicted)

        self._write_count += 1
        if self._write_count >= self._log_writes_period:
            self.log_writes()
            self._write_count = 0

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
