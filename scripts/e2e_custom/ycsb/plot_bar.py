import statistics
import conductor.lib as cond
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_common import COLORS, DATASET_MAP

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 14


def plot_point_queries(data, dataset_filter, show_legend=False):
    fig, ax = plt.subplots(figsize=(4, 3), tight_layout=True)
    relevant = data[
        (data["config"] == "ycsb-{}-64".format(dataset_filter))
        & (data["workload"] != "E")
    ]
    xpos = np.arange(len(relevant["workload"].unique()))
    width = 0.3
    ax.bar(
        xpos - width / 2,
        relevant["kops_per_s_llsm"],
        width,
        color=COLORS["llsm"],
        label="TreeLine",
    )
    ax.bar(
        xpos + width / 2,
        relevant["kops_per_s_rocksdb"],
        width,
        color=COLORS["rocksdb"],
        label="RocksDB",
    )
    ax.set_xlabel("Workload")
    ax.set_ylim((0, 100))
    ax.set_xticks(xpos)
    ax.set_xticklabels(relevant["workload"])

    # Label the speedup above each group
    max_thpts = relevant[["kops_per_s_llsm", "kops_per_s_rocksdb"]].max(axis="columns")
    for x in xpos:
        ax.text(
            x,
            max_thpts.iloc[x] + 2,
            "{:.2f}x".format(relevant["speedup"].iloc[x]),
            horizontalalignment="center",
            fontsize=12,
        )

    if show_legend:
        ax.legend(
            loc="upper right",
            fancybox=False,
            framealpha=1,
            edgecolor="#000000",
            bbox_to_anchor=(1.08, 1.1),
        )
        # To save horizontal space
        ax.set_ylabel("Throughput (krec/s)")
    return fig, ax


def plot_range_queries(data):
    fig, ax = plt.subplots(figsize=(4, 3), tight_layout=True)
    relevant = data[data["workload"] == "E"]
    datasets = list(map(lambda cfg: DATASET_MAP[cfg], data["config"].unique()))
    assert len(relevant) == len(datasets)
    xpos = np.arange(len(datasets))
    width = 0.2
    ax.bar(
        xpos - width / 2,
        relevant["kops_per_s_llsm"],
        width,
        color=COLORS["llsm"],
        label="TreeLine",
    )
    ax.bar(
        xpos + width / 2,
        relevant["kops_per_s_rocksdb"],
        width,
        color=COLORS["rocksdb"],
        label="RocksDB",
    )
    #ax.set_ylabel("Throughput (krec/s)")
    ax.set_xlabel("Dataset")
    ax.set_xlim((-0.5, 2.5))
    ax.set_ylim((0, 400))
    ax.set_xticks(xpos)
    ax.set_xticklabels(datasets)

    # Label the speedup above each group
    max_thpts = relevant[["kops_per_s_llsm", "kops_per_s_rocksdb"]].max(axis="columns")
    for x in xpos:
        ax.text(
            x,
            max_thpts.iloc[x] + 8,
            "{:.2f}x".format(relevant["speedup"].iloc[x]),
            horizontalalignment="center",
            fontsize=12,
        )
    return fig, ax


def process_data(raw_data):
    df = raw_data.copy()
    # This script only plots single threaded results
    df = df[df["threads"] == 1]
    df["kops_per_s"] = df["mops_per_s"] * 1000
    df["workload"] = df["workload"].str.upper()
    relevant_workloads = ["A", "B", "C", "D", "E", "F"]
    df = df[df["workload"].isin(relevant_workloads)]
    llsm = df[df["db"] == "llsm"]
    rocksdb = df[df["db"] == "rocksdb"]
    combined = pd.merge(
        llsm, rocksdb, on=["config", "workload"], suffixes=("_llsm", "_rocksdb")
    )
    thpts_only = combined[
        ["config", "workload", "kops_per_s_llsm", "kops_per_s_rocksdb"]
    ]
    thpts_only["speedup"] = (
        thpts_only["kops_per_s_llsm"] / thpts_only["kops_per_s_rocksdb"]
    )
    return thpts_only


def compute_summary_stats(data, output_file):
    read_heavy_workloads = ["B", "C", "D", "E"]
    overall_speedup = statistics.geometric_mean(data["speedup"])
    read_heavy_bitmap = data["workload"].isin(read_heavy_workloads)
    read_heavy = data[read_heavy_bitmap]
    write_heavy = data[~read_heavy_bitmap]

    read_heavy_speedup = statistics.geometric_mean(read_heavy["speedup"])
    write_heavy_speedup = statistics.geometric_mean(write_heavy["speedup"])

    print("\\newcommand{{\\TreeLineRocksDBSingleAvgSpeedup}}{{${:.2f}\\times$}}".format(overall_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSingleRHAvgSpeedup}}{{${:.2f}\\times$}}".format(read_heavy_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSingleWHAvgSpeedup}}{{${:.2f}\\times$}}".format(write_heavy_speedup), file=output_file)


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    raw_data = pd.read_csv(deps[0] / "all_results.csv")
    data = process_data(raw_data)

    with open(out_dir / "summary_stats.txt", "w") as file:
        compute_summary_stats(data, file)

    fig, _ = plot_point_queries(data, "synthetic", show_legend=True)
    fig.savefig(out_dir / "ycsb-synthetic-64.pdf")

    fig, _ = plot_point_queries(data, "amzn")
    fig.savefig(out_dir / "ycsb-amzn-64.pdf")

    fig, _ = plot_point_queries(data, "osm")
    fig.savefig(out_dir / "ycsb-osm-64.pdf")

    fig, _ = plot_range_queries(data)
    fig.savefig(out_dir / "ycsb-64-range.pdf")


if __name__ == "__main__":
    main()
