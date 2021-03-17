import os
import pathlib

import pandas as pd

pd.options.mode.chained_assignment = None


def main():
    """
    This script combines the ycsb results into one csv file, storing
    each result's name and throughput in both Mops/s and MiB/s.

    This script is meant to be executed by Conductor.
    """
    results_dir = pathlib.Path(os.environ["COND_DEPS"])
    if not results_dir.is_dir():
        raise RuntimeError("Cannot find results!")

    output_dir = pathlib.Path(os.environ["COND_OUT"])
    if not output_dir.is_dir():
        raise RuntimeError("Output directory does not exist!")

    all_results = []

    for record_size_path in results_dir.iterdir():
        for experiment_path in record_size_path.iterdir():
            df = pd.read_csv(experiment_path / "ycsb.csv")
            df["benchmark_name"] = experiment_path.name
            df["mib_per_s"] = df["read_mib_per_s"] + df["write_mib_per_s"]
            results = df[["benchmark_name", "db", "mops_per_s", "mib_per_s"]]
            all_results.append(results)

    combined = pd.concat(all_results)
    combined.sort_values(["benchmark_name", "db"], inplace=True)
    combined.to_csv(output_dir / "ycsb.csv", index=False)


if __name__ == "__main__":
    main()
