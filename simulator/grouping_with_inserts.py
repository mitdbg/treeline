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


def write_summary(segments: List[WritablePageSegment], outfile: pathlib.Path):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
    parser.add_argument("--max_page_records", type=int, default=60)
    parser.add_argument("--max_overflow_frac", type=float, default=0.5)
    parser.add_argument("--initial_dataset_frac", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    if args.workload_config is None and args.custom_dataset is None:
        print("ERROR: Need to provide either a workload config or a custom dataset.")
        return

    random.seed(args.seed)

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

    random.shuffle(dataset)
    initial_load_size = int(len(dataset) * args.initial_dataset_frac)
    initial_load = dataset[:initial_load_size]
    keys_to_insert = dataset[initial_load_size:]

    print("Generating initial segments...", file=sys.stderr)
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
    print("Running insert workload...", file=sys.stderr)
    db = InsertDB(segments, args.records_per_page_goal, args.records_per_page_delta)
    for idx, key in enumerate(keys_to_insert):
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, len(keys_to_insert)))
        db.insert(key, 1)
    db.flatten_all_segments()
    write_summary(db.segments, out_dir / "insert_segments.csv")

    print("Running bulk load workload...", file=sys.stderr)
    bulk_load_segments = list(
        map(
            lambda seg: WritablePageSegment.from_ro_segment(
                seg,
                max_records_per_page=args.max_page_records,
                max_overflow_frac=args.max_overflow_frac,
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
