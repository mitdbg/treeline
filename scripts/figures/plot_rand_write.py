import argparse
import json
import pathlib

import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

from plot_common import RAND_WRITE_COLORS

plt.rcParams["font.size"] = 14
markersize = 12
linewidth = 3.5


# Load and process data given a results directory
def load_and_combine(path):
    results = []
    loc = pathlib.Path(path)

    for exp in loc.iterdir():
        if not exp.is_dir() or exp.name.startswith("."):
            continue

        # Prefix example: write-nvme_ssd_raw-psync-4k
        parts = exp.name.split("-")
        blocksize = parts[-1].upper()
        method = parts[-2]
        fs = parts[-3].split("_")[-1]

        with open(exp / "fio.json", "r") as file:
            raw_json = json.load(file)

        data = aggregate_bw(process_write(raw_json))
        data["fs"] = fs
        data["method"] = method
        data["blocksize"] = blocksize
        results.append(data)

    return pd.concat(results, ignore_index=True)


def process_write(raw_json):
    punits = []
    bw_bytes = []
    lat_ns = []
    for job in raw_json["jobs"]:
        name = job["jobname"]
        parts = name.split("-")
        punits.append(int(parts[-1]))
        bw_bytes.append(int(job["write"]["bw_bytes"]))
        lat_ns.append(int(job["write"]["lat_ns"]["mean"]))

    df = pd.DataFrame({"punits": punits, "bw_bytes": bw_bytes, "lat_ns": lat_ns})
    return df


def aggregate_bw(df):
    results = (
        df.groupby(["punits"])
        .agg({"bw_bytes": "sum", "lat_ns": "median"})
        .reset_index()
    )
    results["bw_mib"] = results["bw_bytes"] / 1024 / 1024
    results["lat_us"] = results["lat_ns"] / 1000
    return results


def block_size_display(bs):
    value = bs[:-1]
    return value + " KiB"


def plot_write_results(df, xlim, peak_seq_bw_mb=1100):
    peak_seqr_bw_mib = peak_seq_bw_mb * 1000 * 1000 / 1024 / 1024
    blocksizes = ["4K", "8K", "16K"]
    markers = ["o", "^", "s"]

    fig, ax = plt.subplots(
        figsize=(6.65, 3),
        tight_layout=True,
        frameon=False,
    )
    for bs, marker in zip(blocksizes, markers):
        rel = df[df["blocksize"] == bs]
        ax.plot(
            rel["punits"],
            rel["bw_mib"],
            marker=marker,
            label=block_size_display(bs),
            color=RAND_WRITE_COLORS[bs],
            markersize=markersize,
            linewidth=linewidth,
        )

    # Represents the peak sequential write bandwidth
    ax.axhline(
        y=peak_seqr_bw_mib,
        linestyle=":",
        linewidth=linewidth,
        color=RAND_WRITE_COLORS["peak"],
    )
    ax.set_ylabel("Throughput (MiB/s)")
    ax.set_xlabel("Threads")
    ax.set_xticks(rel["punits"])
    ax.legend(edgecolor="#000", fancybox=False, loc="lower right")

    ax.set_ylim(0, 1150)
    ax.set_xlim(0, xlim)
    return fig, ax


def main():
    out_dir = cond.get_output_path()
    data = load_and_combine(cond.get_deps_paths()[0])
    fig, _ = plot_write_results(data, xlim=17)
    fig.savefig(out_dir / "rand_write.pdf", format="pdf")


if __name__ == "__main__":
    main()
