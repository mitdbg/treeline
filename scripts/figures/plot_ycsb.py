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
    join_cols = ["dataset", "config", "dist", "workload", "threads"]
    relevant_cols = [*join_cols, "krequests_per_s"]

    pg_llsm = raw_results[raw_results["db"] == "pg_llsm"]
    rocksdb = raw_results[raw_results["db"] == "rocksdb"]
    leanstore = raw_results[raw_results["db"] == "leanstore"]

    pg_llsm_res = pg_llsm[relevant_cols]
    rocksdb_res = rocksdb[relevant_cols]
    leanstore_res = leanstore[relevant_cols]

    pg_llsm_res = pg_llsm_res.rename(
        columns={"krequests_per_s": "krequests_per_s_pg_llsm"}
    )
    rocksdb_res = rocksdb_res.rename(
        columns={"krequests_per_s": "krequests_per_s_rocksdb"}
    )
    leanstore_res = leanstore_res.rename(
        columns={"krequests_per_s": "krequests_per_s_leanstore"}
    )

    combined = pd.merge(pg_llsm_res, rocksdb_res, on=join_cols)
    combined = pd.merge(combined, leanstore_res, on=join_cols)
    combined["speedup_over_rocksdb"] = (
        combined["krequests_per_s_pg_llsm"] / combined["krequests_per_s_rocksdb"]
    )
    combined["speedup_over_leanstore"] = (
        combined["krequests_per_s_pg_llsm"] / combined["krequests_per_s_leanstore"]
    )
    return combined


def plot_scale(
    ax,
    data,
    config,
    dataset,
    workload,
    legend_pos=None,
    show_ylabel=False,
    ylim=None,
    legend_order=None,
):
    relevant = data[
        (data["config"] == config)
        & (data["workload"] == workload)
        & (data["dataset"] == dataset)
    ]

    linewidth = 3.5
    markersize = 15

    # Always plot TreeLine last so that its line is painted on top.
    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_rocksdb"],
        color=COLORS["rocksdb"],
        marker="^",
        linewidth=linewidth,
        markersize=markersize,
        label="RocksDB",
    )
    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_leanstore"],
        color=COLORS["leanstore"],
        marker="s",
        linewidth=linewidth,
        markersize=markersize,
        label="LeanStore",
    )
    ax.plot(
        relevant["threads"],
        relevant["krequests_per_s_pg_llsm"],
        color=COLORS["pg_llsm"],
        marker="o",
        linewidth=linewidth,
        markersize=markersize,
        label="TreeLine",
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


def compute_summary_stats(data, output_file):
    def print_geomean_speedup_stat(name, df, baseline="rocksdb"):
        speedup = statistics.geometric_mean(df["speedup_over_{}".format(baseline)])
        print(
            "\\newcommand{{\\{}}}{{${:.2f}\\times$}}".format(name, speedup),
            file=output_file,
        )

    # All Zipfian "main" point experiments.
    rel = data[
        (data["dist"] == "zipfian")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "e")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianRDBAllPointMainSpeedup", rel)
    print_geomean_speedup_stat(
        "ZipfianLSAllPointMainSpeedup", rel, baseline="leanstore"
    )

    # Amazon Zipfian point workloads (64 B)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "e")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianRDBPointSixtyFourSpeedup", rel)
    print_geomean_speedup_stat(
        "ZipfianLSPointSixtyFourSpeedup", rel, baseline="leanstore"
    )

    # Amazon Zipfian point workloads (1024 B)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["dataset"] == "amzn")
        & (data["workload"] != "e")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("ZipfianRDBPointOneKilobyteSpeedup", rel)
    print_geomean_speedup_stat(
        "ZipfianLSPointOneKilobyteSpeedup", rel, baseline="leanstore"
    )

    print("", file=output_file)

    # 64 B Zipfian Scan
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["workload"] == "e")
    ]
    print_geomean_speedup_stat("ZipfianRDBScanSixtyFourSpeedup", rel)
    print_geomean_speedup_stat(
        "ZipfianLSScanSixtyFourSpeedup", rel, baseline="leanstore"
    )

    # 1024 B Zipfian Scan
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["workload"] == "e")
    ]
    print_geomean_speedup_stat("ZipfianRDBScanOneKilobyteSpeedup", rel)
    print_geomean_speedup_stat(
        "ZipfianLSScanOneKilobyteSpeedup", rel, baseline="leanstore"
    )

    # 64 B Uniform Scan
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "64B")
        & (data["workload"] == "e")
    ]
    print_geomean_speedup_stat("UniformRDBScanSixtyFourSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSScanSixtyFourSpeedup", rel, baseline="leanstore"
    )

    # 1024 B Uniform Scan
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "1024B")
        & (data["workload"] == "e")
    ]
    print_geomean_speedup_stat("UniformRDBScanOneKilobyteSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSScanOneKilobyteSpeedup", rel, baseline="leanstore"
    )

    print("", file=output_file)

    # 64 B Zipfian Scan Only (16)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "64B")
        & (data["workload"] == "e")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("ZipfianRDBScanSixtyFourSpeedupSixteen", rel)
    print_geomean_speedup_stat(
        "ZipfianLSScanSixtyFourSpeedupSixteen", rel, baseline="leanstore"
    )

    # 1024 B Zipfian Scan Only (16)
    rel = data[
        (data["dist"] == "zipfian")
        & (data["config"] == "1024B")
        & (data["workload"] == "e")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("ZipfianRDBScanOneKilobyteSpeedupSixteen", rel)
    print_geomean_speedup_stat(
        "ZipfianLSScanOneKilobyteSpeedupSixteen", rel, baseline="leanstore"
    )

    # 64 B Uniform Scan Only (16)
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "64B")
        & (data["workload"] == "e")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("UniformRDBScanSixtyFourSpeedupSixteen", rel)
    print_geomean_speedup_stat(
        "UniformLSScanSixtyFourSpeedupSixteen", rel, baseline="leanstore"
    )

    # 1024 B Uniform Scan Only (16)
    rel = data[
        (data["dist"] == "uniform")
        & (data["config"] == "1024B")
        & (data["workload"] == "e")
        & (data["threads"] == 16)
    ]
    print_geomean_speedup_stat("UniformRDBScanOneKilobyteSpeedupSixteen", rel)
    print_geomean_speedup_stat(
        "UniformLSScanOneKilobyteSpeedupSixteen", rel, baseline="leanstore"
    )

    print("", file=output_file)

    # All uniform point workloads
    rel = data[
        (data["dist"] == "uniform")
        & (data["workload"] != "e")
        & (data["workload"] != "scan_only")
    ]
    print_geomean_speedup_stat("UniformRDBPointSpeedup", rel)
    print_geomean_speedup_stat("UniformLSPointSpeedup", rel, baseline="leanstore")

    read_heavy = ["b", "c"]
    read_heavy_filter = (data["dist"] == "uniform") & (
        data["workload"].isin(read_heavy)
    )
    write_heavy_filter = (
        (data["dist"] == "uniform")
        & (data["workload"] != "e")
        & (data["workload"] != "scan_only")
        & (~(data["workload"].isin(read_heavy)))
    )

    # Uniform read heavy point workloads
    rel = data[read_heavy_filter]
    print_geomean_speedup_stat("UniformRDBPointReadHeavySpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointReadHeavySpeedup", rel, baseline="leanstore"
    )

    # Uniform write heavy point workloads
    rel = data[write_heavy_filter]
    print_geomean_speedup_stat("UniformRDBPointWriteHeavySpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointWriteHeavySpeedup", rel, baseline="leanstore"
    )

    print("", file=output_file)

    # Uniform read heavy breakdown
    rel = data[read_heavy_filter & (data["config"] == "64B")]
    print_geomean_speedup_stat("UniformRDBPointReadHeavySixtyFourSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointReadHeavySixtyFourSpeedup", rel, baseline="leanstore"
    )
    rel = data[read_heavy_filter & (data["config"] == "1024B")]
    print_geomean_speedup_stat("UniformRDBPointReadHeavyOneKilobyteSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointReadHeavyOneKilobyteSpeedup", rel, baseline="leanstore"
    )

    # Uniform write heavy breakdown
    rel = data[write_heavy_filter & (data["config"] == "64B")]
    print_geomean_speedup_stat("UniformRDBPointWriteHeavySixtyFourSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointWriteHeavySixtyFourSpeedup", rel, baseline="leanstore"
    )
    rel = data[write_heavy_filter & (data["config"] == "1024B")]
    print_geomean_speedup_stat("UniformRDBPointWriteHeavyOneKilobyteSpeedup", rel)
    print_geomean_speedup_stat(
        "UniformLSPointWriteHeavyOneKilobyteSpeedup", rel, baseline="leanstore"
    )

    # Best results
    best_rocksdb = data["speedup_over_rocksdb"].max()
    best_leanstore = data["speedup_over_leanstore"].max()
    print("", file=output_file)
    print(
        "\\newcommand{{\\BestRocksDBSpeedup}}{{${:.2f}\\times$}}".format(best_rocksdb),
        file=output_file,
    )
    print(
        "\\newcommand{{\\BestLeanStoreSpeedup}}{{${:.2f}\\times$}}".format(best_leanstore),
        file=output_file,
    )

    # Best scan results
    best_rocksdb_scan = data[data["workload"] == "e"]["speedup_over_rocksdb"].max()
    best_leanstore_scan = data[data["workload"] == "e"]["speedup_over_leanstore"].max()
    print(
        "\\newcommand{{\\BestRocksDBScanSpeedup}}{{${:.2f}\\times$}}".format(best_rocksdb_scan),
        file=output_file,
    )
    print(
        "\\newcommand{{\\BestLeanStoreScanSpeedup}}{{${:.2f}\\times$}}".format(best_leanstore_scan),
        file=output_file,
    )


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
            legend_order=["TreeLine", "RocksDB", "LeanStore"],
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
        show_legend = dataset == "amzn" and config == "64B" and dist == "uniform"
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
            workload="e",
            legend_pos=("upper left", (-0.035, 1.06)) if show_legend else None,
            show_ylabel=show_ylabel,
            ylim=(0, 120) if config == "64B" else (0, 15),
            legend_order=["TreeLine", "RocksDB", "LeanStore"],
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
