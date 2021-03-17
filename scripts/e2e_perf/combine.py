import os
import pathlib

import pandas as pd


def main():
    """
    This script combines the automated experiment results into one csv file,
    storing each result's name and throughput in both Mops/s and MiB/s.

    This script is meant to be executed by Conductor.
    """
    results_dirs = list(map(pathlib.Path, os.environ["COND_DEPS"].split(":")))
    if any(map(lambda path: not path.is_dir(), results_dirs)):
        raise RuntimeError("Cannot find results!")

    output_dir = pathlib.Path(os.environ["COND_OUT"])
    if not output_dir.is_dir():
        raise RuntimeError("Output directory does not exist!")

    all_results = []

    for results_dir in results_dirs:
        for result_file in results_dir.iterdir():
            if result_file.suffix != ".csv":
                continue
            all_results.append(pd.read_csv(result_file))

    combined = pd.concat(all_results)
    combined.sort_values(["benchmark_name", "db"], inplace=True)
    combined.to_csv(output_dir / "results.csv", index=False)


if __name__ == "__main__":
    main()
