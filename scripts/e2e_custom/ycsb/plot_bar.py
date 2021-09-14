import argparse
import statistics
import pathlib
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
        relevant["kops_per_s_rocksdb_bf"],
        width,
        color=COLORS["rocksdb-bf"],
        label="RocksDB",
    )
    ax.set_xlabel("Workload")
    if (data["dist"] == "uniform").all():
        ax.set_ylim((0, 450))
    else:
        ax.set_ylim((0, 900))
    ax.set_xticks(xpos)
    ax.set_xticklabels(relevant["workload"])

    # Label the speedup above each group
    max_thpts = relevant[["kops_per_s_llsm", "kops_per_s_rocksdb_bf"]].max(axis="columns")
    for x in xpos:
        ax.text(
            x,
            max_thpts.iloc[x] + 15,
            "{:.2f}x".format(relevant["speedup_bf"].iloc[x]),
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
        relevant["kops_per_s_rocksdb_bf"],
        width,
        color=COLORS["rocksdb-bf"],
        label="RocksDB BF",
    )
    #ax.set_ylabel("Throughput (krec/s)")
    ax.set_xlabel("Dataset")
    ax.set_xlim((-0.5, 2.5))
    ax.set_ylim((0, 4000))
    ax.set_xticks(xpos)
    ax.set_xticklabels(datasets)

    # Label the speedup above each group
    max_thpts = relevant[["kops_per_s_llsm", "kops_per_s_rocksdb_bf"]].max(axis="columns")
    for x in xpos:
        ax.text(
            x,
            max_thpts.iloc[x] + 100,
            "{:.2f}x".format(relevant["speedup_bf"].iloc[x]),
            horizontalalignment="center",
            fontsize=12,
        )
    return fig, ax


def process_data(raw_data):
    df = raw_data.copy()
    # This script only plots 16 thread results
    df = df[df["threads"] == 16]
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
    join_on = ["config", "workload", "dist"]
    combined1 = pd.merge(
        llsm, rocksdb, on=join_on, suffixes=("_llsm", "_rocksdb")
    )
    combined = pd.merge(combined1, rocksdb_bf, on=join_on, how="outer")
    thpts_only = combined[
        ["config", "workload", "dist", "kops_per_s_llsm", "kops_per_s_rocksdb", "kops_per_s"]
    ]
    thpts_only = thpts_only.rename(columns={"kops_per_s": "kops_per_s_rocksdb_bf"})
    thpts_only["speedup"] = (
        thpts_only["kops_per_s_llsm"] / thpts_only["kops_per_s_rocksdb"]
    )
    thpts_only["speedup_bf"] = (
        thpts_only["kops_per_s_llsm"] / thpts_only["kops_per_s_rocksdb_bf"]
    )
    return thpts_only


def compute_summary_stats(data, output_file):
    read_heavy_workloads = ["B", "C", "D", "E"]
    # Zipfian only right now
    zipf_bitmap = data["dist"] == "zipfian"
    read_heavy_bitmap = data["workload"].isin(read_heavy_workloads)

    zipf_overall = data[zipf_bitmap]
    read_heavy = data[read_heavy_bitmap & zipf_bitmap]
    write_heavy = data[~read_heavy_bitmap & zipf_bitmap]

    overall_speedup = statistics.geometric_mean(zipf_overall["speedup"])
    overall_speedup_bf = statistics.geometric_mean(zipf_overall["speedup_bf"])
    read_heavy_speedup = statistics.geometric_mean(read_heavy["speedup"])
    read_heavy_speedup_bf = statistics.geometric_mean(read_heavy["speedup_bf"])
    write_heavy_speedup = statistics.geometric_mean(write_heavy["speedup"])
    write_heavy_speedup_bf = statistics.geometric_mean(write_heavy["speedup_bf"])

    print("\\newcommand{{\\TreeLineRocksDBSixteenAvgSpeedup}}{{${:.2f}\\times$}}".format(overall_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSixteenRHAvgSpeedup}}{{${:.2f}\\times$}}".format(read_heavy_speedup), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSixteenWHAvgSpeedup}}{{${:.2f}\\times$}}".format(write_heavy_speedup), file=output_file)
    print("", file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSixteenAvgSpeedupBF}}{{${:.2f}\\times$}}".format(overall_speedup_bf), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSixteenRHAvgSpeedupBF}}{{${:.2f}\\times$}}".format(read_heavy_speedup_bf), file=output_file)
    print("\\newcommand{{\\TreeLineRocksDBSixteenWHAvgSpeedupBF}}{{${:.2f}\\times$}}".format(write_heavy_speedup_bf), file=output_file)


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

    for dist in ["zipfian", "uniform"]:
        rel = data[data["dist"] == dist]
        fig, _ = plot_point_queries(rel, "synthetic", show_legend=(dist == "zipfian"))
        fig.savefig(out_dir / "ycsb-synthetic-64-{}.pdf".format(dist))

        fig, _ = plot_point_queries(rel, "amzn")
        fig.savefig(out_dir / "ycsb-amzn-64-{}.pdf".format(dist))

        fig, _ = plot_point_queries(rel, "osm")
        fig.savefig(out_dir / "ycsb-osm-64-{}.pdf".format(dist))

        fig, _ = plot_range_queries(rel)
        fig.savefig(out_dir / "ycsb-64-range-{}.pdf".format(dist))


if __name__ == "__main__":
    main()
