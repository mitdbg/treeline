import argparse
import statistics
import conductor.lib as cond
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from itertools import product

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 18

COLORS = {
    "llsm": "#023623",
    "rocksdb": "#59FBC1",
}


def process_data(raw_data):
    df = raw_data.copy()
    df["kops_per_s"] = df["mops_per_s"] * 1000
    # Only plot the "middle" workloads with 16 threads
    df = df[df["workload"].str.startswith("middle")]
    df = df[df["threads"] == 16]
    df["insert_pct"] = df["workload"].str.split("-", expand=True)[1].apply(int)
    relevant = df[["config", "workload", "insert_pct", "db", "kops_per_s"]]
    relevant.sort_values(["config", "workload", "insert_pct", "db"], inplace=True)
    return relevant


def compute_summary_stats(data):
    llsm = data[data["db"] == "llsm"].reset_index()
    rocksdb = data[data["db"] == "rocksdb"].reset_index()
    print(llsm["kops_per_s"] / rocksdb["kops_per_s"])


def plot(data):
    fig, ax = plt.subplots(figsize=(9, 3.5), tight_layout=True)
    linewidth=3.5
    markersize=15

    llsm = data[data["db"] == "llsm"]
    rocksdb = data[data["db"] == "rocksdb"]
    ax.plot(
        llsm["insert_pct"],
        llsm["kops_per_s"],
        marker="o",
        markersize=markersize,
        linewidth=linewidth,
        color=COLORS["llsm"],
        label="TreeLine",
    )
    ax.plot(
        rocksdb["insert_pct"],
        rocksdb["kops_per_s"],
        marker="^",
        markersize=markersize,
        linewidth=linewidth,
        color=COLORS["rocksdb"],
        label="RocksDB",
    )
    
    ax.legend(
        loc="upper left",
        fancybox=False,
        framealpha=1,
        edgecolor="#000000",
        bbox_to_anchor=(0, 1.1)
    )
    ax.set_xlabel("Insert Proportion (%)")
    ax.set_ylabel("Throughput (krec/s)")
    ax.set_xlim((10, 90))
    ax.set_xticks([20, 50, 80])
    ax.set_ylim((0, 500))
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
    compute_summary_stats(data)
    fig, ax = plot(data)

    if args.data is not None:
        plt.show()
    else:
        fig.savefig(out_dir / "insert-heavy.pdf")


if __name__ == "__main__":
    main()
