import argparse
import csv
import pathlib
import random
import ycsbr_py as ycsbr
from grouping.insert_db_2 import InsertDB2
from grouping.scan_db import ScanDB
from grouping.merging import MergeStat
from typing import List
import sys

from grouping.loose import (
    build_segments,
    SEGMENT_PAGE_COUNTS,
    SEGMENT_PAGE_COUNTS_TO_INDEX,
)
from grouping.w_segment_2 import WritablePageSegment2
from utils.dataset import extract_keys, load_dataset_from_text_file
from utils.workload_extractor import extract_trace_from_workload


def write_segment_summary(segments: List[WritablePageSegment2], outfile: pathlib.Path):
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


def write_page_counts(counts: List[int], outfile: pathlib.Path):
    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_size", "access_count"])
        for i, c in enumerate(counts):
            # e.g. Segment count 1, access count 1000
            writer.writerow([i + 1, c])


def write_merge_stats(stats: List[MergeStat], outfile: pathlib.Path):
    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segments_in", "segments_out", "pages_in", "pages_out"])
        for s in stats:
            writer.writerow([s.in_segments, s.out_segments, s.in_pages, s.out_pages])


def write_results(db, out_dir: pathlib.Path):
    write_segment_summary(db.segments, out_dir / "segments.csv")
    write_page_counts(db.scan_read_counts, out_dir / "scan_read_counts.csv")
    write_merge_stats(db.merge_stats, out_dir / "merge_stats.csv")

    with open(out_dir / "stats.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["inserts", "merges", "scanned_overflow_pages"]
        )
        writer.writerow(
            [
                db.num_inserts,
                len(db.merge_stats),
                db.scan_overflow_read_counts,
            ]
        )


def run_workload(db, to_insert, to_scan, all_inserts_first):
    idx = 0
    total = len(to_insert) + len(to_scan)
    insert_idx = 0
    scan_idx = 0

    # Interleave the inserts with the scans. Generally there are more inserts
    # than scan requests in the simulations.
    inserts_per_scan = len(to_insert) // len(to_scan)
    if inserts_per_scan <= 0:
        inserts_per_scan = 1

    def maybe_report_progress():
        nonlocal idx
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, total), file=sys.stderr)
        idx += 1

    if not all_inserts_first:
        print("Inserts per scan:", inserts_per_scan, file=sys.stderr)
        while insert_idx < len(to_insert) and scan_idx < len(to_scan):
            for _ in range(inserts_per_scan):
                if insert_idx >= len(to_insert):
                    break
                maybe_report_progress()
                db.insert(to_insert[insert_idx], 0)
                insert_idx += 1

            maybe_report_progress()
            _, start, amount = to_scan[scan_idx]
            db.scan(start, amount)
            scan_idx += 1

    while insert_idx < len(to_insert):
        maybe_report_progress()
        db.insert(to_insert[insert_idx], 0)
        insert_idx += 1

    while scan_idx < len(to_scan):
        maybe_report_progress()
        _, start, amount = to_scan[scan_idx]
        db.scan(start, amount)
        scan_idx += 1

    return db


def main():
    # This simulation runs a insert and scan workload against a database with
    # grouped pages. The purpose of this experiment is to see if we can still
    # achieve scan I/O benefits in the face of inserts.
    #
    # This simulation uses InsertDB2 instead of InsertDB, which uses the "nearby
    # merge" segment merging strategy.

    parser = argparse.ArgumentParser()
    parser.add_argument("--workload_config", type=str, required=True)
    parser.add_argument("--custom_dataset", type=str)

    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=10)

    parser.add_argument("--max_page_records", type=int, default=60)
    parser.add_argument("--max_overflow_pages", type=float, default=1)
    parser.add_argument("--initial_dataset_frac", type=float, default=0.5)

    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out_dir", type=str)

    parser.add_argument("--chains", action="store_true")
    parser.add_argument("--internal_reorg_only", action="store_true")
    parser.add_argument("--all_inserts_first", action="store_true")
    args = parser.parse_args()

    if args.out_dir is None:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    else:
        out_dir = pathlib.Path(args.out_dir)

    workload = ycsbr.PhasedWorkload.from_file(
        args.workload_config,
        set_record_size_bytes=16,
    )
    if args.custom_dataset is not None:
        raw_dataset = load_dataset_from_text_file(args.custom_dataset, shift=False)
        workload.set_custom_load_dataset(raw_dataset)

    dataset = extract_keys(workload.get_load_trace())

    print("Extracting scan workload...", file=sys.stderr)
    scan_trace = extract_trace_from_workload(workload)

    print("Generating initial dataset...", file=sys.stderr)
    random.seed(args.seed)
    random.shuffle(dataset)
    initial_load_size = int(len(dataset) * args.initial_dataset_frac)
    initial_load = dataset[:initial_load_size]
    keys_to_insert = dataset[initial_load_size:]

    if not args.chains:
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

        print("Running workload...", file=sys.stderr)
        db = InsertDB2(
            segments=segments,
            page_goal=args.records_per_page_goal,
            page_delta=args.records_per_page_delta,
            internal_reorg_only=args.internal_reorg_only,
        )
        run_workload(db, keys_to_insert, scan_trace, args.all_inserts_first)
        write_results(db, out_dir)
    else:
        print("Running regular chained workload...", file=sys.stderr)
        db = ScanDB(
            dataset=initial_load,
            keys_per_page_goal=args.records_per_page_goal,
            max_keys_per_page=args.max_page_records,
            reorg_at_length=args.max_overflow_pages + 1,
        )
        run_workload(db, keys_to_insert, scan_trace, args.all_inserts_first)
        with open(out_dir / "stats.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(["inserts", "reorgs", "scanned_pages"])
            writer.writerow(
                [db.num_inserts, db.num_insert_triggered_reorgs, db.scanned_pages]
            )


if __name__ == "__main__":
    main()
