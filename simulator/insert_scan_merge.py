import argparse
import csv
import pathlib
import random
import ycsbr_py as ycsbr
from grouping.insert_db import InsertDB
from typing import List
import sys

from grouping.loose import (
    build_segments,
    SEGMENT_PAGE_COUNTS,
    SEGMENT_PAGE_COUNTS_TO_INDEX,
)
from grouping.w_segment import WritablePageSegment
from utils.dataset import extract_keys, load_dataset_from_text_file
from utils.workload_extractor import extract_trace_from_workload


def write_segment_summary(segments: List[WritablePageSegment], outfile: pathlib.Path):
    counts = [0 for _ in range(len(SEGMENT_PAGE_COUNTS))]
    for segment in segments:
        counts[SEGMENT_PAGE_COUNTS_TO_INDEX[segment.page_count]] += 1

    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_type", "count"])
        writer.writerow(["4k", counts[0]])
        writer.writerow(["8k", counts[1]])
        writer.writerow(["16k", counts[2]])
        writer.writerow(["32k", counts[3]])
        writer.writerow(["64k", counts[4]])

    return counts


def write_page_counts(counts: List[int], outfile: pathlib.Path):
    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_size", "access_count"])
        for i, c in enumerate(counts):
            # e.g. Segment count 1, access count 1000
            writer.writerow([i + 1, c])


def write_results(db, out_dir: pathlib.Path):
    write_segment_summary(db.segments, out_dir / "segments.csv")
    write_page_counts(db.scan_read_counts, out_dir / "scan_read_counts.csv")
    write_page_counts(db.scan_overflow_read_counts, out_dir / "scan_overflow_read_counts.csv")

    with open(out_dir / "stats.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["inserts", "insert_triggered_reorgs", "merges", "successful_merges"])
        writer.writerow([
            db.num_inserts,
            db.num_insert_triggered_reorgs,
            db.num_merges,
            db.num_successful_merges,
        ])


def run_workload(db, to_insert, to_scan):
    idx = 0
    total = len(to_insert) + len(to_scan)
    insert_idx = 0
    scan_idx = 0
    insert_next = True

    # Round-robin replay.
    while insert_idx < len(to_insert) and scan_idx < len(to_scan):
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, total), file=sys.stderr)
        idx += 1

        if insert_next:
            db.insert(to_insert[insert_idx], 0)
            insert_idx += 1
            insert_next = False
        else:
            _, start, amount = to_scan[scan_idx]
            db.scan(start, amount)
            scan_idx += 1
            insert_next = True

    while insert_idx < len(to_insert):
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, total), file=sys.stderr)
        idx += 1

        db.insert(to_insert[insert_idx], 0)
        insert_idx += 1

    while scan_idx < len(to_scan):
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, total), file=sys.stderr)
        idx += 1

        db.scan(start, amount)
        scan_idx += 1

    return db


def main():
    # This simulation runs a insert and scan workload against a database with
    # grouped pages. The purpose of this experiment is to see if we can still
    # achieve scan I/O benefits in the face of inserts.

    parser = argparse.ArgumentParser()
    parser.add_argument("--workload_config", type=str, required=True)
    parser.add_argument("--custom_dataset", type=str)

    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)

    parser.add_argument("--max_page_records", type=int, default=60)
    parser.add_argument("--max_overflow_frac", type=float, default=0.5)
    parser.add_argument("--initial_dataset_frac", type=float, default=0.5)

    parser.add_argument("--merge_times", type=int, default=1000)
    parser.add_argument("--merge_trigger_period", type=int, default=100000)
    parser.add_argument("--slope_epsilon", type=float, default=1e3)

    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out_dir", type=str)
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
        dataset = load_dataset_from_text_file(args.custom_dataset, shift=True)
    else:
        dataset = extract_keys(workload.get_load_trace())

    print("Extracting scan workload...", file=sys.stderr)
    scan_trace = extract_trace_from_workload(workload)

    print("Generating initial segments...", file=sys.stderr)
    random.seed(args.seed)
    random.shuffle(dataset)
    initial_load_size = int(len(dataset) * args.initial_dataset_frac)
    initial_load = dataset[:initial_load_size]
    keys_to_insert = dataset[initial_load_size:]

    segments = list(
        map(
            lambda seg: WritablePageSegment.from_ro_segment(
                seg,
                max_records_per_page=args.max_page_records,
                max_overflow_frac=args.max_overflow_frac,
            ),
            build_segments(
                initial_load,
                goal=args.records_per_page_goal,
                delta=args.records_per_page_delta,
            ),
        )
    )

    print("Running workload...", file=sys.stderr)
    db = InsertDB(
        segments=segments,
        page_goal=args.records_per_page_goal,
        page_delta=args.records_per_page_delta,
        merge_times=args.merge_times,
        merge_trigger_period=args.merge_trigger_period,
        slope_epsilon=args.slope_epsilon,
    )
    run_workload(db, keys_to_insert, scan_trace)
    db.flatten_all_segments()
    write_results(db, out_dir)


if __name__ == "__main__":
    main()
