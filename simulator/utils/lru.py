import collections


class LRUCache:
    # Implementation derived from: https://www.kunxi.org/2014/05/lru-cache-in-python/
    def __init__(self, max_items):
        self._max_items = max_items
        self._cache = collections.OrderedDict()

    def lookup(self, key):
        try:
            value = self._cache.pop(key)
            self._cache[key] = value
            return value
        except KeyError:
            return None

    def add(self, key, value):
        evicted = None
        try:
            self._cache.pop(key)
        except KeyError:
            if len(self._cache) >= self._max_items:
                evicted = self._cache.popitem(last=False)
        self._cache[key] = value
        return evicted

    def items(self):
        return self._cache.items()

    def clear(self):
        return self._cache.clear()
