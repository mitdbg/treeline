import argparse
import csv
import heapq
import json
import sys

from utils.workload_extractor import Op, extract_trace


def compute_next_use(trace):
    # Key to index of next use
    next_use = {}
    not_used_again = len(trace)

    resulting_trace = []
    for idx, req in enumerate(reversed(trace)):
        curr_trace_idx = len(trace) - idx - 1
        key = req[1]
        if key not in next_use:
            resulting_trace.append((req, not_used_again))
        else:
            resulting_trace.append((req, next_use[key]))
        next_use[key] = curr_trace_idx

    return list(reversed(resulting_trace))


def validate_next_use(trace):
    for idx, ((_, key), next_use) in enumerate(trace):
        assert next_use > idx
        if next_use >= len(trace):
            # All other requests from after `idx` are not `key` (i.e., the key
            # never appears again in the trace).
            assert all(map(lambda req: req[0][0] != key, trace[idx + 1 :]))
            continue

        # The key at index `next_use` is equal to `key`
        assert trace[next_use][0][1] == key
        # All other requests from after `idx` to just before `next_use` are not
        # `key` (i.e., `next_use` is the actual next use index).
        assert all(map(lambda req: req[0][1] != key, trace[idx + 1 : next_use]))


class BeladyCache:
    class _EvictionMetadata:
        def __init__(self, key, next_use):
            self.key = key
            self.next_use = next_use

        def __lt__(self, other):
            return (-self.next_use) < (-other.next_use)

    def __init__(self, capacity):
        self._capacity = capacity
        self._eviction_order = []
        # Key -> Eviction Metadata
        self._cache = {}
        self._heapified = False

    def lookup(self, key):
        return key in self._cache

    def add_or_update(self, key, next_use):
        if self.lookup(key):
            self._cache[key].next_use = next_use
            if self._heapified:
                # Need to maintain the heap property
                heapq.heapify(self._eviction_order)
            return False

        if len(self._cache) < self._capacity:
            assert not self._heapified
            ev = self._EvictionMetadata(key, next_use)
            self._cache[key] = ev
            self._eviction_order.append(ev)
            return True

        # Need to evict.
        if not self._heapified:
            heapq.heapify(self._eviction_order)
            self._heapified = True

        # Always evict the key that is next used fartest in the future (Python's
        # heaps are min-heaps).
        new_item = self._EvictionMetadata(key, next_use)
        evicted = heapq.heapreplace(self._eviction_order, new_item)
        del self._cache[evicted.key]
        self._cache[key] = new_item
        return True


def replay_trace(trace_with_next_use, cache_capacity):
    accesses = 0
    hits = 0
    read_accesses = 0
    read_hits = 0
    write_accesses = 0
    write_hits = 0

    cache = BeladyCache(cache_capacity)
    for req, next_use in trace_with_next_use:
        op, key = req
        found = cache.lookup(key)

        accesses += 1
        if found:
            hits += 1

        if op == Op.Read:
            read_accesses += 1
            if found:
                read_hits += 1
        else:
            write_accesses += 1
            if found:
                write_hits += 1

        cache.add_or_update(key, next_use)

    return {
        "hit_rate": hits / accesses,
        "read_hit_rate": read_hits / read_accesses,
        "write_hit_rate": write_hits / write_accesses,
    }


def main():
    # This script simulates a Bélády cache replacement strategy on a workload
    # trace. Its purpose is to provide a comparison point for implementable
    # cache eviction strategies.
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config")
    parser.add_argument("--cache_capacity", type=int, required=True)
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--print_csv", action="store_true")
    args = parser.parse_args()

    trace = extract_trace(args.workload_config)
    with_next_use = compute_next_use(trace)

    if args.validate:
        validate_next_use(with_next_use)

    results = replay_trace(with_next_use, args.cache_capacity)
    if args.print_csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(["hit_rate", "read_hit_rate", "write_hit_rate"])
        writer.writerow(
            [results["hit_rate"], results["read_hit_rate"], results["write_hit_rate"]]
        )
    else:
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
