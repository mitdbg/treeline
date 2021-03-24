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
    results_dirs = list(map(pathlib.Path, os.environ["COND_DEPS"].split(":")))
    if any(map(lambda path: not path.is_dir(), results_dirs)):
        raise RuntimeError("Cannot find results!")

    output_dir = pathlib.Path(os.environ["COND_OUT"])
    if not output_dir.is_dir():
        raise RuntimeError("Output directory does not exist!")

    all_results = []

    for combined_experiment_path in results_dirs:
        for experiment_path in combined_experiment_path.iterdir():
            df = pd.read_csv(experiment_path / "ycsb.csv")
            # We append a "-llsm" or "-rocksdb" suffix to the benchmark name to
            # keep the experiments for LLSM and RocksDB separate (for
            # Conductor). Since we record the DB type in the outputted CSV
            # file, we do not need it as a part of the benchmark name in the
            # summarized results.
            df["benchmark_name"] = experiment_path.name.split("-")[0]
            df["mib_per_s"] = df["read_mib_per_s"] + df["write_mib_per_s"]
            results = df[["benchmark_name", "db", "mops_per_s", "mib_per_s"]]
            all_results.append(results)

    combined = pd.concat(all_results)
    combined.sort_values(["benchmark_name", "db"], inplace=True)
    combined.to_csv(output_dir / "ycsb.csv", index=False)


if __name__ == "__main__":
    main()
