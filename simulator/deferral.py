import argparse
import bisect
import csv
import statistics
import pathlib

from utils.workload_extractor import extract_workload


class MemtablePage:
    def __init__(self):
        self.keys = None
        self.defer_count = 0


class SimulatedMemtable:
    def __init__(self, page_boundaries, replace_dups=True):
        self._replace_dups = replace_dups
        self._page_boundaries = page_boundaries

        # page -> MemtablePage
        self._buckets = {}
        self._size = 0

    @property
    def size(self):
        return self._size

    @property
    def records_by_page(self):
        return self._buckets

    def add_record(self, key):
        page = self._page_for_key(key)
        if page not in self._buckets:
            self._buckets[page] = MemtablePage()
            if self._replace_dups:
                self._buckets[page].keys = {key}
            else:
                self._buckets[page].keys = [key]
            self._size += 1
            return

        keys = self._buckets[page].keys
        if self._replace_dups:
            size_before = len(keys)
            keys.add(key)
            if len(keys) != size_before:
                self._size += 1
        else:
            keys.append(key)
            self._size += 1

    def remove_above_threshold(self, threshold, records_per_page):
        write_amps = []
        to_remove = []

        # Scan for removal
        for page_id, page in self._buckets.items():
            if not self._replace_dups:
                deduped = set(page.keys)
            else:
                deduped = page.keys
            page_threshold = len(deduped) / records_per_page
            if page_threshold >= threshold:
                # Should remove
                to_remove.append(page_id)
                # Write amplification: Physical data written / logical data to write
                write_amps.append(records_per_page / len(deduped))
            else:
                page.defer_count += 1

        # Do the removal
        for page_to_remove in to_remove:
            self._size -= len(self._buckets[page_to_remove].keys)
            del self._buckets[page_to_remove]

        assert self._size >= 0
        return write_amps

    def remove_all(self, records_per_page):
        write_amps = []
        for page in self._buckets.values():
            if not self._replace_dups:
                deduped = set(page.keys)
            else:
                deduped = page.keys
            write_amps.append(records_per_page / len(deduped))
        self._buckets.clear()
        self._size = 0
        return write_amps

    def remove_defer_count(self, defer_count, records_per_page):
        write_amps = []
        to_remove = []

        # Scan for removal
        for page_id, page in self._buckets.items():
            if page.defer_count <= defer_count:
                continue
            if not self._replace_dups:
                deduped = set(page.keys)
            else:
                deduped = page.keys
            # Should remove
            to_remove.append(page_id)
            # Write amplification: Physical data written / logical data to write
            write_amps.append(records_per_page / len(deduped))

        # Do the removal
        for page_to_remove in to_remove:
            self._size -= len(self._buckets[page_to_remove].keys)
            del self._buckets[page_to_remove]

        assert self._size >= 0
        return write_amps

    def _page_for_key(self, key):
        return bisect.bisect_left(self._page_boundaries, key)


class Simulator:
    def __init__(self, db):
        self._dataset = db.dataset
        self._updates = db.update_trace
        self._reads = db.read_trace
        self._page_boundaries = []
        self._key_update_freqs = {}
        self._page_update_freqs = {}

    def generate_pages(self, keys_per_page):
        self._dataset.sort()
        self._page_boundaries.clear()
        count = 0
        for key in self._dataset:
            if count == 0:
                self._page_boundaries.append(key)
            count += 1
            if count >= keys_per_page:
                count = 0

    def compute_update_key_freqs(self):
        self._key_update_freqs.clear()
        for update_key in self._updates:
            if update_key in self._key_update_freqs:
                self._key_update_freqs[update_key] += 1
            else:
                self._key_update_freqs[update_key] = 1

    def compute_update_page_freqs(self):
        self._page_update_freqs.clear()
        for update_key, freq in self._key_update_freqs.items():
            page = self._page_for_key(update_key)
            if page in self._page_update_freqs:
                self._page_update_freqs[page] += freq
            else:
                self._page_update_freqs[page] = freq

    def run_deferral(
        self,
        threshold,
        defer_count,
        records_per_page,
        memtable_records,
        replace_dups=True,
    ):
        """Computes the average write amplification for a given deferral threshold."""
        # `threshold` should be a value in [0, 1.0]
        # `records_per_page` is the number of records in a page
        # `memtable_records` is the number of records we can hold in memory
        write_amps = []
        memtable = SimulatedMemtable(self._page_boundaries, replace_dups)
        num_flushes = 0
        num_forceful_evictions = 0
        num_page_writes = 0
        num_defer_drops = 0
        trace_len = len(self._updates)
        for idx, update_key in enumerate(self._updates):
            if idx % 1000000 == 0:
                print("[{}/{}] running deferral simulation...".format(idx, trace_len))

            memtable.add_record(update_key)
            if memtable.size >= memtable_records:
                # Run deferral
                print("Running deferral")
                num_flushes += 1
                flushed_wamps = memtable.remove_above_threshold(
                    threshold, records_per_page
                )
                write_amps.extend(flushed_wamps)
                num_page_writes += len(flushed_wamps)

                defer_dropped = memtable.remove_defer_count(
                    defer_count, records_per_page
                )
                write_amps.extend(defer_dropped)
                num_page_writes += len(defer_dropped)
                num_defer_drops += len(defer_dropped)

                if memtable.size >= memtable_records:
                    # Flush did not find any pages above the threshold. We now
                    # forcefully write out all pages.
                    num_forceful_evictions += 1
                    forced_wamps = memtable.remove_all(records_per_page)
                    write_amps.extend(forced_wamps)
                    num_page_writes += len(forced_wamps)
                assert memtable.size < memtable_records

        return (
            num_flushes,
            num_forceful_evictions,
            num_page_writes,
            num_defer_drops,
            write_amps,
        )

    def _page_for_key(self, key):
        return bisect.bisect_left(self._page_boundaries, key)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config", type=str)
    parser.add_argument("--record_size_bytes", type=int, default=64)
    parser.add_argument("--keys_per_page", type=int, default=32)
    parser.add_argument("--memtable_size_mib", type=int, default=64)
    parser.add_argument(
        "--flush_thresholds",
        type=str,
        default="0.05,0.1,0.15,0.2,0.25",
    )
    parser.add_argument("--accumulate_dups", action="store_true")
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    print(args.workload_config)

    memtable_records = int(args.memtable_size_mib * 1024 * 1024 / args.record_size_bytes)
    flush_thresholds = list(map(float, args.flush_thresholds.split(",")))

    db = extract_workload(
        workload_config_file=args.workload_config,
        record_size_bytes=args.record_size_bytes,
    )
    sim = Simulator(db)
    sim.generate_pages(args.keys_per_page)

    if args.out_dir is None:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(args.out_dir)

    with open(out_dir / "results.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "flush_fill_threshold",
                "num_flushes",
                "num_evicts",
                "num_page_writes",
                "num_defer_drops",
                "mean_wamp",
                "min_wamp",
                "max_wamp",
            ]
        )

        for thresh in flush_thresholds:
            print("Running with threshold:", thresh)
            (
                num_flushes,
                num_evicts,
                num_page_writes,
                num_defer_drops,
                write_amps,
            ) = sim.run_deferral(
                thresh,
                defer_count=1,  # N.B. This is hardcoded!
                records_per_page=args.keys_per_page,
                memtable_records=memtable_records,
                replace_dups=(not args.accumulate_dups),
            )
            writer.writerow(
                [
                    thresh,
                    num_flushes,
                    num_evicts,
                    num_page_writes,
                    num_defer_drops,
                    statistics.mean(write_amps) if len(write_amps) > 0 else 0,
                    min(*write_amps) if len(write_amps) > 0 else 0,
                    max(*write_amps) if len(write_amps) > 0 else 0,
                ]
            )


if __name__ == "__main__":
    main()
