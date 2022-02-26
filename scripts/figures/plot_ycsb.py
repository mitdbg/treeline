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
    def print_geomean_speedup_stat(name, df):
        speedup = statistics.geometric_mean(df["speedup"])
        print(
            "\\newcommand{{\\{}}}{{${:.2f}\\times$}}".format(name, speedup),
            file=output_file,
        )

    # All Zipfian "main" point experiments.
    rel = data[
        (data["dist"] == "zipfian")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianAllPointMainSpeedup", rel)

    # Amazon Zipfian point workloads (64 B)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianPointSixtyFourSpeedup", rel)

    # Amazon Zipfian point workloads (1024 B)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianPointOneKilobyteSpeedup", rel)

    print("", file=output_file)

    # 64 B Zipfian Scan Only
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["workload"] == "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianScanSixtyFourSpeedup", rel)

    # 1024 B Zipfian Scan Only
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["workload"] == "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianScanOneKilobyteSpeedup", rel)

    # 64 B Uniform Scan Only
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "64B")
        & (data["workload"] == "scan_only")
    ]
    print_geomean_speedup_stat("UniformScanSixtyFourSpeedup", rel)

    # 1024 B Uniform Scan Only
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "1024B")
        & (data["workload"] == "scan_only")
    ]
    print_geomean_speedup_stat("UniformScanOneKilobyteSpeedup", rel)

    print("", file=output_file)

    # 64 B Zipfian Scan Only (16)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["workload"] == "scan_only")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("ZipfianScanSixtyFourSpeedupSixteen", rel)

    # 1024 B Zipfian Scan Only (16)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["workload"] == "scan_only")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("ZipfianScanOneKilobyteSpeedupSixteen", rel)

    # 64 B Uniform Scan Only (16)
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "64B")
        & (data["workload"] == "scan_only")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("UniformScanSixtyFourSpeedupSixteen", rel)

    # 1024 B Uniform Scan Only (16)
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "1024B")
        & (data["workload"] == "scan_only")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("UniformScanOneKilobyteSpeedupSixteen", rel)

    print("", file=output_file)

    # All uniform point workloads
    rel = data[(data["dist"] == "uniform") & (data["workload"] != "scan_only")]
    print_geomean_speedup_stat("UniformPointSpeedup", rel)

    read_heavy = ["b", "c"]
    read_heavy_filter = ((data["dist"] == "uniform") & (
        data["workload"].isin(read_heavy))
    )
    write_heavy_filter = ((data["dist"] == "uniform")
        & (data["workload"] != "scan_only")
        & (~(data["workload"].isin(read_heavy))))

    # Uniform read heavy point workloads
    rel = data[read_heavy_filter]
    print_geomean_speedup_stat("UniformPointReadHeavySpeedup", rel)

    # Uniform write heavy point workloads
    rel = data[write_heavy_filter]
    print_geomean_speedup_stat("UniformPointWriteHeavySpeedup", rel)

    print("", file=output_file)

    # Uniform read heavy breakdown
    rel = data[read_heavy_filter & (data["config"] == "64B")]
    print_geomean_speedup_stat("UniformPointReadHeavySixtyFourSpeedup", rel)
    rel = data[read_heavy_filter & (data["config"] == "1024B")]
    print_geomean_speedup_stat("UniformPointReadHeavyOneKilobyteSpeedup", rel)

    # Uniform write heavy breakdown
    rel = data[write_heavy_filter & (data["config"] == "64B")]
    print_geomean_speedup_stat("UniformPointWriteHeavySixtyFourSpeedup", rel)
    rel = data[write_heavy_filter & (data["config"] == "1024B")]
    print_geomean_speedup_stat("UniformPointWriteHeavyOneKilobyteSpeedup", rel)


def make_point_plots(args, data, out_dir):
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
        if args.data is not None:
            # Just save one as a preview.
            fig.savefig(out_dir / "amzn-{}-{}.pdf".format(config, workload))
            break
        elif args.data is None:
            fig.savefig(out_dir / "amzn-{}-{}.pdf".format(config, workload))
        plt.close(fig)


def make_scan_plots(args, data, out_dir):
    # Create the scan workload plots.
    datasets = ["amzn", "osm", "synth"]
    configs = ["64B", "1024B"]
    for config, dataset, dist in product(configs, datasets, ["zipfian", "uniform"]):
        rel = data[data["dist"] == dist]
        show_legend = dataset == "amzn" and config == "64B" and dist == "zipfian"
        show_ylabel = dataset == "amzn" and config == "64B"
        if show_legend or show_ylabel:
            fig, ax = plt.subplots(figsize=(5, 3.2), tight_layout=True)
        else:
            fig, ax = plt.subplots(figsize=(4, 3.2), tight_layout=True)
        plot_scale(
            ax,
            rel,
            config=config,
            dataset=dataset,
            workload="scan_only",
            legend_pos=("upper left", (-0.035, 1.06)) if show_legend else None,
            show_ylabel=show_ylabel,
            ylim=(0, 90) if config == "64B" else (0, 15),
        )
        if args.data is not None:
            # Just save one as a preview.
            fig.savefig(out_dir / "scan-{}-{}-{}.pdf".format(dataset, config, dist))
            break
        elif args.data is None:
            fig.savefig(out_dir / "scan-{}-{}-{}.pdf".format(dataset, config, dist))
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", nargs="*")
    parser.add_argument("--summary-only", action="store_true")
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

    if args.summary_only:
        return

    make_point_plots(args, data, out_dir)
    make_scan_plots(args, data, out_dir)


if __name__ == "__main__":
    main()
