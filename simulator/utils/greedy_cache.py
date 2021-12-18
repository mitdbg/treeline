import enum
import ycsbr_py as ycsbr

from utils.dataset import process_dataset


class CacheItemType(enum.Enum):
    Read = 1
    Write = 2


class CacheItem:
    MAX_HOTNESS = 8

    def __init__(self, item_type, hotness):
        self.item_type = item_type
        self.hotness = hotness

    def incr_hotness(self):
        self.hotness = min(self.hotness + 1, CacheItem.MAX_HOTNESS)

    def decr_hotness(self):
        self.hotness = max(self.hotness - 1, 0)

    def set_type(self, item_type):
        self.item_type = item_type


class GreedyCache:
    def __init__(self, page_mapper, capacity):
        self._page_mapper = page_mapper
        self._capacity = capacity

        self._records = {}
        self._page_to_records = {}
        self._slots = []
        self._arm = 0

        self._read_ios = 0
        self._write_ios = 0

        self._accesses = 0
        self._hits = 0

    @property
    def hit_rate(self):
        return self._hits / self._accesses

    @property
    def read_ios(self):
        return self._read_ios

    @property
    def write_ios(self):
        return self._write_ios

    def add_write(self, key, if_new_hotness):
        self._accesses += 1
        if key in self._records:
            self._hits += 1
            self._records[key].incr_hotness()
            self._records[key].set_type(CacheItemType.Write)
            return

        slot_idx = len(self._slots)
        if len(self._records) >= self._capacity:
            slot_idx = self._evict()

        # Guaranteed to have space now.
        self._add_new_item(
            key, CacheItem(CacheItemType.Write, if_new_hotness), slot_idx
        )

    def lookup(self, key):
        self._accesses += 1
        if key not in self._records:
            return False
        self._hits += 1
        self._records[key].incr_hotness()
        return True

    def add_read(self, key, if_new_hotness):
        if key in self._records:
            return

        slot_idx = len(self._slots)
        if len(self._records) >= self._capacity:
            slot_idx = self._evict()

        # Guaranteed to have space now.
        self._add_new_item(key, CacheItem(CacheItemType.Read, if_new_hotness), slot_idx)

    def _evict(self):
        # Age cache items until we find one with count 0
        # - If it is a read item - evict
        # - If it is a write item, keep aging until you complete one cycle
        #   - If you encounter a read item, evict
        #   - Otherwise, evict the write records belonging to the largest batch
        #     (by page). Evict one record, make the rest "read" items now.
        start_idx = self._arm

        evict_candidate = None
        evict_candidate_slot = None
        flush_size = 0

        while (start_idx != self._arm) or (evict_candidate is None):
            key = self._slots[self._arm]
            item = self._records[key]

            # Eviction candidate if the hotness is 0
            if item.hotness == 0:
                page = self._page_mapper(key)
                if item.item_type == CacheItemType.Read:
                    # Evict right away because it is a read
                    self._evict_key(key, page)
                    free_slot = self._arm
                    self._advance_arm()
                    return free_slot
                else:
                    # This record is a write; maybe evict it
                    num_on_page = len(self._page_to_records[page])
                    if evict_candidate is None or num_on_page > flush_size:
                        evict_candidate = key
                        evict_candidate_slot = self._arm
                        flush_size = num_on_page
            else:
                item.decr_hotness()

            # Always advance the arm
            self._advance_arm()

        # We are evicting a write record. Otherwise we would have returned above.
        assert self._records[evict_candidate].item_type == CacheItemType.Write

        page = self._page_mapper(evict_candidate)
        self._evict_key(evict_candidate, page)
        # Requires a read-modify-write.
        self._read_ios += 1
        self._write_ios += 1

        # Other records on the page in the cache are no longer dirty
        if page in self._page_to_records:
            for key in self._page_to_records[page]:
                self._records[key].set_item_type(CacheItemType.Read)

        return evict_candidate_slot

    def _add_new_item(self, key, cache_item, slot_idx):
        page = self._page_mapper(key)
        self._records[key] = cache_item
        if page in self._page_to_records:
            self._page_to_records[page].add(key)
        else:
            self._page_to_records[page] = {key}

        if slot_idx >= len(self._slots):
            self._slots.append(key)
            assert len(self._slots) <= self._capacity
        else:
            self._slots[slot_idx] = key

    def _advance_arm(self):
        self._arm += 1
        if self._arm >= len(self._slots):
            self._arm = 0

    def _evict_key(self, key, page):
        del self._records[key]
        self._page_to_records[page].remove(key)
        if len(self._page_to_records[page]) == 0:
            del self._page_to_records[page]


class GreedyCacheDB(ycsbr.DatabaseInterface):
    Name = "greedy"

    def __init__(self, dataset, keys_per_page, cache_capacity, admit_read_pages=False):
        ycsbr.DatabaseInterface.__init__(self)
        page_mapper, page_data = process_dataset(dataset, keys_per_page)
        self._page_mapper = page_mapper
        self._page_data = page_data
        self._cache = GreedyCache(page_mapper, cache_capacity)
        self._admit_read_pages = admit_read_pages
        self._read_ios = 0
        self._write_ios = 0

    @property
    def read_ios(self):
        return self._read_ios + self._cache.read_ios

    @property
    def write_ios(self):
        return self._write_ios + self._cache.write_ios

    @property
    def hit_rate(self):
        return self._cache.hit_rate

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
        self._cache.add_write(key, if_new_hotness=4)
        return True

    def read(self, key):
        if self._cache.lookup(key):
            # Already in the cache
            return

        # Read the record and add it to the cache
        self._read_ios += 1
        if self._admit_read_pages:
            for page_key in self._page_data[self._page_mapper(key)]:
                if page_key == key:
                    # The key being requested is added with hotness 4
                    self._cache.add_read(page_key, if_new_hotness=4)
                else:
                    # Other keys in the page are added with hotness 1
                    self._cache.add_read(page_key, if_new_hotness=1)
        else:
            # Just admit the key
            self._cache.add_read(key, if_new_hotness=4)

    def scan(self, start, amount):
        return []
