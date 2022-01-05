import argparse
import csv
import pathlib
import sys
import ycsbr_py as ycsbr

from grouping.loose import build_segments
from utils.dataset import extract_keys
from utils.run import run_workload
from utils.segmented_db import SegmentedDB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=10)
    args = parser.parse_args()

    workload = ycsbr.PhasedWorkload.from_file(
        args.workload_config,
        set_record_size_bytes=16,
    )
    if args.custom_dataset is not None:
        workload.set_custom_load_dataset(args.custom_dataset)
    dataset = extract_keys(workload.get_load_trace())

    print("Building the segments...", file=sys.stderr)
    segments = build_segments(
        dataset, args.records_per_page_goal, args.records_per_page_delta
    )

    # Run with segments
    print("Running segmented workload...", file=sys.stderr)
    segmented_db = SegmentedDB(segments)
    run_workload(workload, segmented_db)

    config_path = pathlib.Path(args.workload_config)
    page_size_counts = map(lambda p: "count_{}".format(str(p + 1)), range(16))
    with open("segmented.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["workload", "goal", "delta", *page_size_counts])
        writer.writerow(
            [
                config_path.stem,
                args.records_per_page_goal,
                args.records_per_page_delta,
                *segmented_db.read_counts,
            ]
        )


if __name__ == "__main__":
    main()
