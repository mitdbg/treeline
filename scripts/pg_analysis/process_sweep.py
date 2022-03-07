import shutil
import re

import conductor.lib as cond
import pandas as pd

EXPERIMENT_REGEX = re.compile("(?P<const_param>[a-zA-Z]+)_(?P<const_val>[0-9]+)\.task")


def main():
    out_dir = cond.get_output_path()
    deps = cond.get_deps_paths()

    breakdown_dir = out_dir / "seg_breakdowns"
    breakdown_dir.mkdir(exist_ok=True)

    results = []

    for dep in deps:
        res = EXPERIMENT_REGEX.match(dep.name)
        assert res is not None

        const_param = res.group("const_param")
        const_value = int(res.group("const_val"))
        vary_param = "goal" if const_param == "delta" else "delta"

        for exp_inst in dep.iterdir():
            if not exp_inst.is_dir() or exp_inst.name.startswith("."):
                continue
            parts = exp_inst.name.split("-")
            config = "-".join(parts[1:])
            dataset = parts[1]
            vary = int(parts[2])

            seg_breakdown_file = exp_inst / "debug" / "segment_summary.csv"
            shutil.copy2(seg_breakdown_file, breakdown_dir / "{}.csv".format(config))
            seg_breakdown = pd.read_csv(seg_breakdown_file)

            # Compute the fraction of pages that are in a segment
            seg_breakdown["num_pages"] = (
                seg_breakdown["segment_page_count"] * seg_breakdown["num_segments"]
            )
            total_pages = seg_breakdown["num_pages"].sum()
            pages_in_segments = seg_breakdown[seg_breakdown["segment_page_count"] > 1][
                "num_pages"
            ].sum()
            in_segment_fraction = pages_in_segments / total_pages

            # Sanity check.
            counters = pd.read_csv(exp_inst / "counters.csv")
            segment_index_entries = counters[counters["name"] == "segments"][
                "value"
            ].iloc[0]
            assert seg_breakdown["num_segments"].sum() == segment_index_entries

            # Compute reduction in entries (ratio) induced by using segments (higher is better).
            entries_reduction = total_pages / segment_index_entries

            results.append((dataset, vary, in_segment_fraction, entries_reduction))

        # Write out combined results for this particular parameter sweep.
        combined = pd.DataFrame.from_records(
            results,
            columns=[
                "dataset",
                vary_param,
                "in_segment_fraction",
                "entries_reduction_ratio",
            ],
        )
        combined.sort_values(
            by=["dataset", vary_param], inplace=True, ignore_index=True
        )
        combined.to_csv(
            out_dir / "{}_{}.csv".format(const_param, const_value), index=False
        )
        results.clear()


if __name__ == "__main__":
    main()
