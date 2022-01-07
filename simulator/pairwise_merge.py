import argparse
import csv
import pathlib
import random
from sortedcontainers import SortedKeyList
import ycsbr_py as ycsbr
from typing import List, Union
import sys

from grouping.loose import (
    build_segments,
    build_single_segments,
    SEGMENT_PAGE_COUNTS,
    SEGMENT_PAGE_COUNTS_TO_INDEX,
)
from grouping.ro_segment import ReadOnlyPageSegment
from grouping.w_segment import WritablePageSegment
from grouping.merging import try_pairwise_merge
from utils.dataset import extract_keys, load_dataset_from_text_file


def write_summary(
    segments: Union[List[WritablePageSegment], List[ReadOnlyPageSegment]],
    outfile: pathlib.Path,
):
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


def run_pairwise_merge(segments: SortedKeyList, args):
    successful_merges = 0
    for idx in range(args.iterations):
        if idx % 1000000 == 0:
            print("{}/{}".format(idx, args.iterations))

        candidate = random.randrange(0, len(segments) - 1)
        left = segments[candidate]
        right = segments[candidate + 1]
        results = try_pairwise_merge(
            left, right,
            slope_epsilon=args.slope_epsilon,
            page_goal=args.records_per_page_goal,
            page_delta=args.records_per_page_delta,
        )
        if len(results) != 1:
            # Unsuccessful merge.
            continue
        successful_merges += 1
        segments.remove(left)
        segments.remove(right)
        segments.add(results[0])
    return successful_merges


def main():
    # This experiment is for testing the pairwise merge strategy. We start with
    # 4 KiB segments and see how much "merging" we can do for a fixed number of
    # iterations. We compare the final segment distribution against the segments
    # created from bulk load of the full dataset.
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
    parser.add_argument("--slope_epsilon", type=float)
    parser.add_argument("--iterations", type=int)
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

    if args.iterations is None:
        # Running bulk load only.
        print("Running bulk load only...", file=sys.stderr)
        segments = build_segments(
            dataset,
            goal=args.records_per_page_goal,
            delta=args.records_per_page_delta,
        )
        write_summary(segments, out_dir / "bulk_load.csv")
        return

    print("Generating single page segments...", file=sys.stderr)
    segments = build_single_segments(
        dataset,
        goal=args.records_per_page_goal,
        delta=args.records_per_page_delta,
    )
    segments = SortedKeyList(
        map(
            lambda s: WritablePageSegment.from_ro_segment(
                # The two kwargs are placeholders. They are unused by this experiment.
                s, max_records_per_page=60, max_overflow_frac=0.5
            ),
            segments,
        ),
        key=lambda seg: seg.base_key,
    )

    print(
        "Running merge workload for {} iterations...".format(args.iterations),
        file=sys.stderr,
    )
    successes = run_pairwise_merge(segments, args)
    print("Success count:", successes)
    write_summary(segments, out_dir / "after_merge.csv")


if __name__ == "__main__":
    main()
