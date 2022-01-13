import argparse
import csv
import pathlib
import random
import sortedcontainers
import ycsbr_py as ycsbr
from typing import List, Iterable
import sys

from grouping.loose import (
    build_segments,
    SEGMENT_PAGE_COUNTS,
    SEGMENT_PAGE_COUNTS_TO_INDEX,
)
from grouping.merging import greedy_merge_at, MergeStat
from grouping.w_segment_2 import WritablePageSegment2
from utils.dataset import extract_keys, load_dataset_from_text_file


def write_summary(segments: List[WritablePageSegment2], outfile: pathlib.Path):
    counts = [0 for _ in range(len(SEGMENT_PAGE_COUNTS) + 1)]
    for segment in segments:
        counts[SEGMENT_PAGE_COUNTS_TO_INDEX[segment.segment_size]] += 1
        # Count overflows separately, but they are all "4k pages"
        counts[-1] += segment.overflow_page_count

    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_type", "count"])
        writer.writerow(["4k", counts[0]])
        writer.writerow(["8k", counts[1]])
        writer.writerow(["16k", counts[2]])
        writer.writerow(["32k", counts[3]])
        writer.writerow(["64k", counts[4]])
        writer.writerow(["overflow_4k", counts[5]])

    return counts


def write_merge_stats(stats: List[MergeStat], outfile: pathlib.Path):
    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segments_in", "segments_out", "pages_in", "pages_out"])
        for s in stats:
            writer.writerow([s.in_segments, s.out_segments, s.in_pages, s.out_pages])


class DB:
    def __init__(
        self,
        segments: List[WritablePageSegment2],
        page_goal: int,
        page_delta: int,
        internal_reorg_only: bool,
    ):
        self._segments = sortedcontainers.SortedKeyList(
            segments, key=lambda seg: seg.base_key
        )
        self._page_goal = page_goal
        self._page_delta = page_delta
        self._internal_reorg_only = internal_reorg_only
        self.merge_stats: List[MergeStat] = []

    def insert(self, key):
        seg_idx = self._segment_for_key(key)
        seg = self._segments[seg_idx]
        succeeded = seg.insert(key)
        if succeeded:
            return

        # Need to run a reorg/merge.
        res = greedy_merge_at(
            self._segments,
            seg_idx,
            self._page_goal,
            self._page_delta,
            single_only=self._internal_reorg_only,
        )
        assert res.remove_count > 0
        assert len(res.new_segments) > 0
        removed_segments = []
        for _ in range(res.remove_count):
            removed_segments.append(self._segments.pop(res.remove_start_idx))
        for s in res.new_segments:
            self._segments.add(s)
        self.merge_stats.append(
            MergeStat(
                in_segments=res.remove_count,
                in_pages=self._page_count(removed_segments),
                out_segments=len(res.new_segments),
                out_pages=self._page_count(res.new_segments),
            )
        )

        # Redo the insert. It must succeed.
        seg_idx = self._segment_for_key(key)
        seg = self._segments[seg_idx]
        succeeded = seg.insert(key)
        assert succeeded

    def _segment_for_key(self, key):
        idx = self._segments.bisect_key_right(key)
        if idx > 0:
            idx -= 1
        return idx

    @staticmethod
    def _page_count(segments: Iterable[WritablePageSegment2]) -> int:
        return sum(map(lambda seg: seg.segment_size, segments))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
    parser.add_argument("--max_page_records", type=int, default=60)
    parser.add_argument("--max_overflow_pages", type=float, default=1)
    parser.add_argument("--initial_dataset_frac", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run_bulk_load", action="store_true")
    # If set, don't do merging across segments.
    parser.add_argument("--internal_reorg_only", action="store_true")
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    if args.workload_config is None and args.custom_dataset is None:
        print("ERROR: Need to provide either a workload config or a custom dataset.")
        return

    if args.out_dir is None:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(args.out_dir)

    if args.workload_config is not None:
        workload = ycsbr.PhasedWorkload.from_file(
            args.workload_config,
            set_record_size_bytes=16,
        )
        dataset = extract_keys(workload.get_load_trace())
    else:
        assert args.custom_dataset is not None
        dataset = load_dataset_from_text_file(args.custom_dataset, shift=True)

    if not args.run_bulk_load:
        random.seed(args.seed)
        random.shuffle(dataset)
        initial_load_size = int(len(dataset) * args.initial_dataset_frac)
        initial_load = dataset[:initial_load_size]
        keys_to_insert = dataset[initial_load_size:]

        print("Generating initial segments...", file=sys.stderr)
        segments = list(
            map(
                lambda seg: WritablePageSegment2.from_ro_segment(
                    seg,
                    max_records_per_page=args.max_page_records,
                    max_overflow_pages=args.max_overflow_pages,
                ),
                build_segments(
                    initial_load,
                    goal=args.records_per_page_goal,
                    delta=args.records_per_page_delta,
                ),
            )
        )
        print("Running insert workload...", file=sys.stderr)
        db = DB(
            segments,
            args.records_per_page_goal,
            args.records_per_page_delta,
            args.internal_reorg_only,
        )
        for idx, key in enumerate(keys_to_insert):
            if idx % 1000000 == 0:
                print("{}/{}".format(idx, len(keys_to_insert)))
            db.insert(key)
        write_summary(db._segments, out_dir / "insert_segments.csv")
        write_merge_stats(db.merge_stats, out_dir / "merge_stats.csv")

    else:
        print("Running bulk load workload...", file=sys.stderr)
        bulk_load_segments = list(
            map(
                lambda seg: WritablePageSegment2.from_ro_segment(
                    seg,
                    max_records_per_page=args.max_page_records,
                    max_overflow_pages=args.max_overflow_pages,
                ),
                build_segments(
                    dataset,
                    goal=args.records_per_page_goal,
                    delta=args.records_per_page_delta,
                ),
            )
        )
        write_summary(bulk_load_segments, out_dir / "bulk_load_segments.csv")


if __name__ == "__main__":
    main()
