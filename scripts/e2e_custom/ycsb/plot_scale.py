import argparse
import statistics
import conductor.lib as cond
import matplotlib.pyplot as plt
import pathlib
import numpy as np
import pandas as pd
from itertools import product

from plot_common import COLORS, DATASET_MAP

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 18


def process_data(raw_data):
    df = raw_data.copy()
    df["kops_per_s"] = df["mops_per_s"] * 1000
    df["workload"] = df["workload"].str.upper()
    df["dist"] = df["workload"].apply(lambda w: "uniform" if w.startswith("UN_") else "zipfian")
    df["workload"] = df["workload"].str.lstrip("UN_")
    df["db"] = df.apply(lambda row: row["db"] + "-bf" if row["config"].endswith("rdbb") else row["db"], axis=1)
    df["config"] = df["config"].str.rstrip("-rdbb")
    llsm = df[df["db"] == "llsm"]
    rocksdb = df[df["db"] == "rocksdb"]
    rocksdb_bf = df[df["db"] == "rocksdb-bf"]
    # Three way join
    join_on = ["config", "workload", "dist", "threads"]
    combined1 = pd.merge(
        llsm, rocksdb, on=join_on, suffixes=("_llsm", "_rocksdb")
    )
    combined = pd.merge(combined1, rocksdb_bf, on=join_on, how="outer")
    thpts_only = combined[[
        "config",
        "workload",
        "dist",
        "threads",
        "kops_per_s_llsm",
        "kops_per_s_rocksdb",
        "kops_per_s",
    ]]
    thpts_only = thpts_only.rename(columns={"kops_per_s": "kops_per_s_rocksdb_bf"})
    thpts_only["speedup"] = (
        thpts_only["kops_per_s_llsm"] / thpts_only["kops_per_s_rocksdb"]
    )
    thpts_only["speedup_bf"] = (
        thpts_only["kops_per_s_llsm"] / thpts_only["kops_per_s_rocksdb_bf"]
    )
    thpts_only = thpts_only.sort_values(by=["config", "workload", "dist", "threads"])
    return thpts_only


def plot_scale(data, dataset, workload, show_legend=False, show_ylabel=False):
    if show_legend or show_ylabel:
        fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
    else:
        fig, ax = plt.subplots(figsize=(4, 3.2), tight_layout=True)
    relevant = data[
        (data["config"] == "ycsb-{}-64".format(dataset))
        & (data["workload"] == workload)
    ]

    linewidth=3.5
    markersize=15

    ax.plot(
        relevant["threads"],
        relevant["kops_per_s_llsm"],
        color=COLORS["llsm"],
        marker="o",
        linewidth=linewidth,
        markersize=markersize,
        label="TreeLine",
    )
    ax.plot(
        relevant["threads"],
        relevant["kops_per_s_rocksdb"],
        color=COLORS["rocksdb"],
        marker="^",
        linewidth=linewidth,
        markersize=markersize,
        label="RocksDB NB",
    )
    ax.plot(
        relevant["threads"],
        relevant["kops_per_s_rocksdb_bf"],
        color=COLORS["rocksdb-bf"],
        marker="d",
        linewidth=linewidth,
        markersize=markersize,
        label="RocksDB",
    )

    ax.set_xlabel("Threads")
    ax.set_xticks(relevant["threads"])

    if show_legend:
        ax.legend(
            loc="lower right",
            fancybox=False,
            framealpha=1,
            edgecolor="#000000",
            bbox_to_anchor=(1.08, -0.1)
        )
    if show_ylabel:
        ax.set_ylabel("Throughput (krec/s)")

    return fig, ax


def compute_summary_stats(data, output_file):
    zipf_data = data[data["dist"] == "zipfian"]
    uni_data = data[data["dist"] == "uniform"]

    # Zipfian results
    overall_speedup = statistics.geometric_mean(zipf_data["speedup"])
    at16_speedup = statistics.geometric_mean(zipf_data[zipf_data["threads"] == 16]["speedup"])
    print("\\newcommand{{\\TreeLineRocksDBMultiAvgSpeedup}}{{${:.2f}\\times$}}".format(overall_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBMultiSixteenAvgSpeedup}}{{${:.2f}\\times$}}".format(at16_speedup), file=output_file)
    print("", file=output_file)

    # Zipfian results over the RocksDB w/ bloom filter
    overall_speedup_bf = statistics.geometric_mean(zipf_data["speedup_bf"])
    at16_speedup_bf = statistics.geometric_mean(zipf_data[zipf_data["threads"] == 16]["speedup_bf"])
    print("\\newcommand{{\\TreeLineRocksDBMultiAvgSpeedupBF}}{{${:.2f}\\times$}}".format(overall_speedup_bf), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBMultiSixteenAvgSpeedupBF}}{{${:.2f}\\times$}}".format(at16_speedup_bf), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBMultiMaxSpeedupBF}}{{${:.2f}\\times$}}".format(zipf_data["speedup_bf"].max()), file=output_file)
    print("", file=output_file)

    # Uniform results
    uni_overall_speedup = statistics.geometric_mean(uni_data["speedup"])
    uni_at16_speedup = statistics.geometric_mean(uni_data[uni_data["threads"] == 16]["speedup"])
    print("\\newcommand{{\\TreeLineRocksDBMultiAvgSpeedupUni}}{{${:.2f}\\times$}}".format(uni_overall_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBMultiSixteenAvgSpeedupUni}}{{${:.2f}\\times$}}".format(uni_at16_speedup), file=output_file)

    # Uniform results over RocksDB w/ bloom filter
    uni_overall_speedup_bf = statistics.geometric_mean(uni_data["speedup_bf"])
    uni_at16_speedup_bf = statistics.geometric_mean(uni_data[uni_data["threads"] == 16]["speedup_bf"])
    print("\\newcommand{{\\TreeLineRocksDBMultiAvgSpeedupUniBF}}{{${:.2f}\\times$}}".format(uni_overall_speedup_bf), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBMultiSixteenAvgSpeedupUniBF}}{{${:.2f}\\times$}}".format(uni_at16_speedup_bf), file=output_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", nargs="*")
    args = parser.parse_args()

    if args.data is None:
        deps = cond.get_deps_paths()
        out_dir = cond.get_output_path()
        dfs = [pd.read_csv(dep / "all_results.csv") for dep in deps]
    else:
        out_dir = pathlib.Path(".")
        dfs = [pd.read_csv(file) for file in args.data]

    raw_data = pd.concat(dfs, ignore_index=True)
    data = process_data(raw_data)

    with open(out_dir / "summary_stats.txt", "w") as file:
        compute_summary_stats(data, file)

    # Zipfian only right now
    data = data[data["dist"] == "zipfian"]
    datasets = ["synthetic", "amzn", "osm"]
    workloads = ["A", "B", "C", "D", "E", "F"]

    for dataset, workload in product(datasets, workloads):
        show_legend = dataset == "synthetic" and workload == "A"
        fig, _ = plot_scale(
            data,
            dataset,
            workload,
            show_legend,
            show_ylabel=workload == "A",
        )
        if show_legend and args.data is not None:
            plt.show()
            return
        elif args.data is None:
            fig.savefig(out_dir / "ycsb-{}-64-{}.pdf".format(dataset, workload))


if __name__ == "__main__":
    main()
