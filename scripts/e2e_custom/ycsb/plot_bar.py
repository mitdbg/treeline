import conductor.lib as cond
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 14

COLORS = {
    "llsm": "#0876B6",
    "rocksdb": "#59C1FB",
}

DATASET_MAP = {
    "ycsb-synthetic-64": "Synthetic",
    "ycsb-amzn-64": "Amazon",
    "ycsb-osm-64": "OSM",
}


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
        label="LLSM (ours)",
    )
    ax.bar(
        xpos + width / 2,
        relevant["kops_per_s_rocksdb"],
        width,
        color=COLORS["rocksdb"],
        label="RocksDB",
    )
    ax.set_ylabel("Throughput (kops/s)")
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
        )
    return fig, ax


def plot_range_queries(data):
    fig, ax = plt.subplots(figsize=(3, 3), tight_layout=True)
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
        label="LLSM (ours)",
    )
    ax.bar(
        xpos + width / 2,
        relevant["kops_per_s_rocksdb"],
        width,
        color=COLORS["rocksdb"],
        label="RocksDB",
    )
    ax.set_ylabel("Throughput (krecords/s)")
    ax.set_xlabel("Workload")
    ax.set_xlim((-0.5, 1.5))
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
    df["kops_per_s"] = df["mops_per_s"] * 1000
    df["workload"] = df["workload"].str.upper()
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


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    raw_data = pd.read_csv(deps[0] / "all_results.csv")
    data = process_data(raw_data)

    fig, _ = plot_point_queries(data, "synthetic", show_legend=True)
    fig.savefig(out_dir / "ycsb-synthetic-64.pdf")

    fig, _ = plot_point_queries(data, "amzn")
    fig.savefig(out_dir / "ycsb-amzn-64.pdf")

    fig, _ = plot_range_queries(data)
    fig.savefig(out_dir / "ycsb-64-range.pdf")


if __name__ == "__main__":
    main()
