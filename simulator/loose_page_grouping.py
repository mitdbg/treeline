import argparse
import csv
import pathlib

from grouping.loose import (
    build_segments,
    validate_segments,
    SEGMENT_PAGE_COUNTS,
    SEGMENT_PAGE_COUNTS_TO_INDEX,
)
from utils.dataset import extract_keys, load_dataset_from_text_file


def write_summary(segments, out_dir):
    counts = [0 for _ in range(len(SEGMENT_PAGE_COUNTS))]
    for segment in segments:
        counts[SEGMENT_PAGE_COUNTS_TO_INDEX[segment.page_count]] += 1

    with open(out_dir / "summary.csv", "w") as f:
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
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=5)
    parser.add_argument("--workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--validate", action="store_true")
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

    if args.custom_dataset is not None:
        dataset = load_dataset_from_text_file(args.custom_dataset)
    else:
        import ycsbr_py as ycsbr

        workload = ycsbr.PhasedWorkload.from_file(
            args.workload_config,
            set_record_size_bytes=16,
        )
        dataset = extract_keys(workload.get_load_trace())

    segments = build_segments(
        dataset, goal=args.records_per_page_goal, delta=args.records_per_page_delta
    )
    write_summary(segments, out_dir)

    if args.validate:
        validate_segments(
            segments, goal=args.records_per_page_goal, delta=args.records_per_page_delta
        )


if __name__ == "__main__":
    main()
