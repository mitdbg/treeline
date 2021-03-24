import os
import pathlib

import pandas as pd


def compute_speedup_over_rocksdb(results):
    """
    Given a dataframe with the raw results (LLSM and RocksDB), removes all
    RocksDB rows and replaces them with a new column "speedup_over_rocksdb".
    """
    llsm = results[results["db"] == "llsm"]
    rocksdb = results[results["db"] == "rocksdb"][
        ["benchmark_name", "mops_per_s"]
    ]
    # Duplicate the RocksDB results and append "_defer" to each benchmark's
    # name. This helps us compute LLSM speedups when deferral is used.
    rocksdb_defer = rocksdb.copy(deep=True)
    rocksdb_defer["benchmark_name"] = (
        rocksdb_defer["benchmark_name"].apply(lambda name: name + "_defer")
    )
    rocksdb = pd.concat([rocksdb, rocksdb_defer])

    joined = pd.merge(
        llsm,
        rocksdb,
        on="benchmark_name",
        how="left",
        suffixes=["_llsm", "_rocksdb"],
    )
    joined["speedup_over_rocksdb"] = (
        joined["mops_per_s_llsm"] / joined["mops_per_s_rocksdb"]
    )
    relevant = joined[[
        "benchmark_name",
        "mops_per_s_llsm",
        "mib_per_s",
        "speedup_over_rocksdb",
    ]]
    return relevant.rename(columns={"mops_per_s_llsm": "mops_per_s"})


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
    combined.to_csv(output_dir / "raw_results.csv", index=False)

    results = compute_speedup_over_rocksdb(combined)
    results.to_csv(output_dir/ "results.csv", index=False)


if __name__ == "__main__":
    main()
