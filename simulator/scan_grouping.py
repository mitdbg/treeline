import argparse
import csv
import pathlib
import sys
import ycsbr_py as ycsbr

from grouping.loose import build_segments
from grouping.segmented_scan_db import SegmentedScanDB
from grouping.scan_db import ScanDB
from utils.dataset import extract_keys, load_dataset_from_text_file
from utils.run import run_workload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config", type=str)
    parser.add_argument("--custom_dataset", type=str)
    parser.add_argument("--records_per_page_goal", type=int, default=50)
    parser.add_argument("--records_per_page_delta", type=int, default=10)
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
        raw_loaded_dataset = load_dataset_from_text_file(args.custom_dataset)
        workload.set_custom_load_dataset(raw_loaded_dataset)
    dataset = extract_keys(workload.get_load_trace())

    print("Building the segments...", file=sys.stderr)
    segments = build_segments(
        dataset, args.records_per_page_goal, args.records_per_page_delta
    )

    print("Running with segments...", file=sys.stderr)
    segmented_db = SegmentedScanDB(segments)
    run_workload(workload, segmented_db)

    print("Running with simple pages...")
    scan_db = ScanDB(dataset, keys_per_page=args.records_per_page_goal)
    run_workload(workload, scan_db)

    config_path = pathlib.Path(args.workload_config)
    page_size_counts = map(lambda p: "count_{}".format(str(p + 1)), range(16))
    with open(out_dir / "results.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["workload", "goal", "delta", *page_size_counts, "simple_scan_page_count"])
        writer.writerow(
            [
                config_path.stem,
                args.records_per_page_goal,
                args.records_per_page_delta,
                *segmented_db.read_counts,
                scan_db.scanned_pages,
            ]
        )


if __name__ == "__main__":
    main()
