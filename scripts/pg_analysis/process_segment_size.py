import argparse
import shutil

import conductor.lib as cond
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--segment-info-bytes", type=int, default=32)
    parser.add_argument("--page-value-bytes", type=int, default=8)
    args = parser.parse_args()

    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    seg_dist_dir = out_dir / "seg_dist"
    seg_dist_dir.mkdir(exist_ok=True)

    with_segs = []
    no_segs = []

    # Extract the relevant counters and copy over the segment summaries.
    for dep in deps:
        for exp_inst in dep.iterdir():
            if not exp_inst.is_dir() or exp_inst.name.startswith("."):
                continue
            # e.g. segment_size-amzn-1024B-no_segs
            parts = exp_inst.name.split("-")
            dataset = parts[1]
            config = parts[2]
            uses_segs = parts[3] == "segs"
            config_name = "-".join(parts[1:])

            counters = pd.read_csv(exp_inst / "counters.csv")
            num_segments = counters[counters["name"] == "segments"]["value"].iloc[0]
            index_size = counters[counters["name"] == "segment_index_bytes"][
                "value"
            ].iloc[0]

            if uses_segs:
                with_segs.append((dataset, config, num_segments, index_size))

                # Copy over the segment distribution.
                shutil.copy2(
                    exp_inst / "debug" / "segment_summary.csv",
                    seg_dist_dir / "{}.csv".format(config_name),
                )
            else:
                no_segs.append((dataset, config, num_segments, index_size))

    common_columns = [
        "dataset",
        "config",
    ]

    with_segs_df = pd.DataFrame.from_records(
        with_segs,
        columns=[
            *common_columns,
            "num_segments",
            "index_size_bytes_segs",
        ],
    )
    no_segs_df = pd.DataFrame.from_records(
        no_segs, columns=[*common_columns, "num_pages", "index_size_bytes_pages"]
    )
    combined = pd.merge(with_segs_df, no_segs_df, on=common_columns)
    combined["est_segment_values_size"] = combined["num_segments"] * args.segment_info_bytes
    combined["est_page_values_size"] = combined["num_pages"] * args.page_value_bytes

    # Write out the merged results.
    combined.to_csv(out_dir / "segment_summary.csv", index=False)


if __name__ == "__main__":
    main()
