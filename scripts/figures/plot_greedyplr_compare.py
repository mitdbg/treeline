import argparse
import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd
from itertools import product
from scipy.stats import gmean

from plot_common import COLORS

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 18


def plot_scale(
    ax,
    data,
    config,
    workload,
    legend_pos=None,
    show_ylabel=False,
    ylim=None,
    legend_order=None,
):
    relevant = data[(data["config"] == config) & (data["workload"] == workload)]

    linewidth = 3.5
    markersize = 15

    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_plr"],
        color=COLORS["rocksdb"],
        marker="^",
        linewidth=linewidth,
        markersize=markersize,
        label="GreedyPLR",
    )
    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_orig"],
        color=COLORS["pg_llsm"],
        marker="o",
        linewidth=linewidth,
        markersize=markersize,
        label="PGM",
    )

    ax.set_xlabel("Threads")
    ax.set_xticks(relevant["threads"])

    if legend_pos is not None:
        legend_options = dict(
            loc=legend_pos[0],
            fancybox=False,
            framealpha=0.5,
            edgecolor="#000000",
            bbox_to_anchor=legend_pos[1],
        )
        if legend_order is not None:
            handles, labels = ax.get_legend_handles_labels()
            reordered = []
            for lbl in legend_order:
                idx = labels.index(lbl)
                reordered.append(handles[idx])
            ax.legend(reordered, legend_order, **legend_options)
        else:
            ax.legend(**legend_options)

    if show_ylabel:
        ax.set_ylabel("Throughput (kreq/s)")

    if ylim is not None:
        ax.set_ylim(ylim)


def make_point_plots(data, out_dir):
    # Create the point workload main plots.
    workloads = ["a", "b", "c", "d", "f"]
    configs = ["64B", "1024B"]
    for config, workload in product(configs, workloads):
        show_legend = workload == "a" and config == "64B"
        if workload == "a":
            fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
        else:
            fig, ax = plt.subplots(figsize=(4, 3.2), tight_layout=True)
        plot_scale(
            ax,
            data,
            config=config,
            workload=workload,
            legend_pos=("upper left", (-0.035, 1.06)) if show_legend else None,
            show_ylabel=workload == "a",
            ylim=(0, 1700),
            legend_order=["PGM", "GreedyPLR"],
        )
        fig.savefig(out_dir / "amzn_greedyplr-{}-{}.pdf".format(config, workload))
        plt.close(fig)


def make_scan_plots(data, out_dir):
    # Create the scan workload plots.
    configs = ["64B", "1024B"]
    for config in configs:
        show_legend = False
        show_ylabel = False
        if show_legend or show_ylabel:
            fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
        else:
            fig, ax = plt.subplots(figsize=(4, 3.2), tight_layout=True)
        plot_scale(
            ax,
            data,
            config=config,
            workload="e",
            legend_pos=("upper left", (-0.035, 1.06)) if show_legend else None,
            show_ylabel=show_ylabel,
            ylim=(0, 100) if config == "64B" else (0, 15),
            legend_order=["PGM", "GreedyPLR"],
        )
        fig.savefig(out_dir / "amzn_greedyplr-{}-e.pdf".format(config))
        plt.close(fig)


def process_data(raw_data):
    orig = raw_data[raw_data["dataset"] == "amzn"]
    plr = raw_data[raw_data["dataset"] == "amzn_greedyplr"]
    orig_rel = orig[
        (orig["dataset"] == "amzn")
        & (orig["db"] == "pg_llsm")
        & (orig["dist"] == "zipfian")
    ]
    comb = pd.merge(
        orig_rel,
        plr,
        on=["config", "dist", "db", "workload", "threads"],
        suffixes=["_orig", "_plr"],
    )
    comb["ratio"] = comb["krequests_per_s_plr"] / comb["krequests_per_s_orig"]
    return comb


def compute_summary(data):
    ratios = data["ratio"]
    geomean = gmean(ratios)
    print("Geomean GreedyPLR speedup:", geomean)
    print("Max. GreedyPLR speedup:", ratios.max())
    print("Min. GreedyPLR speedup:", ratios.min())

    ratios_e = data[data["workload"] == "e"]["ratio"]
    geomean_e = gmean(ratios_e)
    print("Geomean E GreedyPLR speedup:", geomean_e)
    print("Max. E GreedyPLR speedup:", ratios_e.max())
    print("Min. E GreedyPLR speedup:", ratios_e.min())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats-only", action="store_true")
    args = parser.parse_args()

    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()
    dfs = [pd.read_csv(dep / "all_results.csv") for dep in deps]

    raw_data = pd.concat(dfs, ignore_index=True)
    data = process_data(raw_data)

    compute_summary(data)

    if args.stats_only:
        return

    make_point_plots(data, out_dir)
    make_scan_plots(data, out_dir)


if __name__ == "__main__":
    main()
