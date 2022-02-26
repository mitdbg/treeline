import argparse
import statistics
import conductor.lib as cond
import matplotlib.pyplot as plt
import pathlib
import pandas as pd
from itertools import product

from plot_common import COLORS, DATASET_MAP

pd.options.mode.chained_assignment = None
plt.rcParams["font.size"] = 18


def process_data(raw_results):
    pg_llsm = raw_results[raw_results["db"] == "pg_llsm"]
    rocksdb = raw_results[raw_results["db"] == "rocksdb"]
    pg_llsm_res = pg_llsm[
        ["dataset", "config", "dist", "workload", "threads", "krequests_per_s"]
    ]
    rocksdb_res = rocksdb[
        ["dataset", "config", "dist", "workload", "threads", "krequests_per_s"]
    ]
    combined = pd.merge(
        pg_llsm_res,
        rocksdb_res,
        on=["dataset", "config", "dist", "workload", "threads"],
        suffixes=["_pg_llsm", "_rocksdb"],
    )
    combined["speedup"] = (
        combined["krequests_per_s_pg_llsm"] / combined["krequests_per_s_rocksdb"]
    )
    return combined


def plot_scale(
    ax, data, config, dataset, workload, legend_pos=None, show_ylabel=False, ylim=None
):
    relevant = data[
        (data["config"] == config)
        & (data["workload"] == workload)
        & (data["dataset"] == dataset)
    ]

    linewidth = 3.5
    markersize = 15

    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_pg_llsm"],
        color=COLORS["pg_llsm"],
        marker="o",
        linewidth=linewidth,
        markersize=markersize,
        label="TreeLine",
    )
    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_rocksdb"],
        color=COLORS["rocksdb"],
        marker="^",
        linewidth=linewidth,
        markersize=markersize,
        label="RocksDB",
    )

    ax.set_xlabel("Threads")
    ax.set_xticks(relevant["threads"])

    if legend_pos is not None:
        ax.legend(
            loc=legend_pos[0],
            fancybox=False,
            framealpha=1,
            edgecolor="#000000",
            bbox_to_anchor=legend_pos[1],
        )
    if show_ylabel:
        ax.set_ylabel("Throughput (kreq/s)")

    if ylim is not None:
        ax.set_ylim(ylim)


def compute_summary_stats(data, output_file):
    ## All Zipfian "main" experiments.
    rel1 = data[
        (data["dist"] == "zipfian")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "scan_only")
    ]
    rel2 = data[(data["dist"] == "zipfian") & (data["workload"] == "scan_only")]
    rel = pd.concat([rel1, rel2], ignore_index=True)
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\ZipfianAllMainSpeedup}}{{${:.2f}\\times$}}".format(speedup),
        file=output_file,
    )

    ## Amazon overall.
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["dataset"] == "amzn")
    ]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\AmazonZipfSpeedupSixtyFourBytes}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )

    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["dataset"] == "amzn")
    ]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\AmazonZipfSpeedupOneKilobyte}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )
    print("", file=output_file)

    ## Zipfian scan only (across datasets).
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["workload"] == "scan_only")
    ]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\ZipfScanSpeedupSixtyFourBytes}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )
    rel = rel[rel["threads"] == 16]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\ZipfScanSixteenSpeedupSixtyFourBytes}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )

    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["workload"] == "scan_only")
    ]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\ZipfScanSpeedupOneKilobyte}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )
    rel = rel[rel["threads"] == 16]
    speedup = statistics.geometric_mean(rel["speedup"])
    print(
        "\\newcommand{{\\ZipfScanSixteenSpeedupOneKilobyte}}{{${:.2f}\\times$}}".format(
            speedup
        ),
        file=output_file,
    )


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

    # Get the summary stats.
    with open(out_dir / "summary_stats.txt", "w") as file:
        compute_summary_stats(data, file)

    # Create the point workload main plots.
    workloads = ["a", "b", "c", "d", "f"]
    configs = ["64B", "1024B"]
    for config, workload in product(configs, workloads):
        rel = data[data["dist"] == "zipfian"]
        show_legend = workload == "a" and config == "64B"
        fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
        plot_scale(
            ax,
            rel,
            config=config,
            dataset="amzn",
            workload=workload,
            legend_pos=("upper left", (-0.035, 1.06)) if show_legend else None,
            show_ylabel=workload == "a",
            ylim=(0, 1700),
        )
        if show_legend and args.data is not None:
            # Just save one as a preview.
            fig.savefig(out_dir / "amzn-{}-{}.pdf".format(config, workload))
            break
        elif args.data is None:
            fig.savefig(out_dir / "amzn-{}-{}.pdf".format(config, workload))

    # Create the scan workload plots.
    datasets = ["amzn", "osm", "synth"]
    for config, dataset in product(configs, datasets):
        rel = data[data["dist"] == "zipfian"]
        show_legend = dataset == "amzn" and config == "64B"
        if show_legend:
            fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
        else:
            fig, ax = plt.subplots(figsize=(4, 3.2), tight_layout=True)
        plot_scale(
            ax,
            rel,
            config=config,
            dataset=dataset,
            workload="scan_only",
            legend_pos=("lower right", (1.02, -0.03)) if show_legend else None,
            show_ylabel=show_legend,
            ylim=(0, 80) if config == "64B" else (0, 12),
        )
        if args.data is not None:
            # Just save one as a preview.
            fig.savefig(out_dir / "scan-{}-{}.pdf".format(dataset, config))
            break
        elif args.data is None:
            fig.savefig(out_dir / "scan-{}-{}.pdf".format(dataset, config))


if __name__ == "__main__":
    main()
