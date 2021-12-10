import argparse
import bisect
import pathlib
import pandas as pd
import random

from utils.workload_extractor import extract_workload


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

    def _page_for_key(self, key):
        return bisect.bisect_left(self._page_boundaries, key)


def sort_freqs(counts):
    total_count = 0
    pairs = []
    for key, freq in counts.items():
        total_count += freq
        pairs.append((key, freq))
    # Sort by frequency of access in descending order
    pairs.sort(key=lambda pair: -pair[1])
    return pairs


def to_csv(pairs, filepath):
    keys = []
    freqs = []
    for key, freq in pairs:
        keys.append(key)
        freqs.append(freq)
    df = pd.DataFrame({"key": keys, "freq": freqs})
    df.to_csv(filepath, index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config", type=str)
    parser.add_argument("--record_size_bytes", type=int, default=64)
    parser.add_argument("--keys_per_page", type=int, default=32)
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()
    random.seed(42)

    db = extract_workload(args.workload_config, args.record_size_bytes)
    sim = Simulator(db)
    sim.generate_pages(args.keys_per_page)

    sim.compute_update_key_freqs()
    sim.compute_update_page_freqs()

    if args.out_dir is None:
        import conductor.lib as cond
        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(args.out_dir)

    keys = sort_freqs(sim._key_update_freqs)
    to_csv(keys, out_dir / "keys.csv")

    pages = sort_freqs(sim._page_update_freqs)
    to_csv(pages, out_dir / "pages.csv")


if __name__ == "__main__":
    main()
