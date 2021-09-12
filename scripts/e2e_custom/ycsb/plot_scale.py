import argparse
import conductor.lib as cond
import matplotlib.pyplot as plt
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
    llsm = df[df["db"] == "llsm"]
    rocksdb = df[df["db"] == "rocksdb"]
    combined = pd.merge(
        llsm, rocksdb, on=["config", "workload", "threads"], suffixes=("_llsm", "_rocksdb")
    )
    thpts_only = combined[
        ["config", "workload", "threads", "kops_per_s_llsm", "kops_per_s_rocksdb"]
    ]
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
        )
    if show_ylabel:
        ax.set_ylabel("Throughput (krec/s)")

    return fig, ax


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=False)
    args = parser.parse_args()

    if args.data is None:
        deps = cond.get_deps_paths()
        out_dir = cond.get_output_path()
        raw_data = pd.read_csv(deps[0] / "all_results.csv")
    else:
        raw_data = pd.read_csv(args.data)

    data = process_data(raw_data)

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
        elif args.data is None:
            fig.savefig(out_dir / "ycsb-{}-64-{}.pdf".format(dataset, workload))


if __name__ == "__main__":
    main()
