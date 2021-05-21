import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams["font.size"] = 14


DISTRIBUTIONS = ["uniform", "zipfian"]
DB_FORMAT = {
    "llsm": "LLSM (ours)",
    "rocksdb": "RocksDB",
}


def plot_e2e(df, distribution, show_legend=False):
    fig, ax = plt.subplots(
        figsize=(6.65, 4),
        tight_layout=True,
        frameon=False,
    )
    dbs = df["db"].unique()
    # (memtable_mib, cache_mib)
    configs = [(64, 3200), (3200, 64)]
    for db in dbs:
        for (memtable_mib, cache_mib) in configs:
            data = df[(df["db"] == db) &
                      (df["memtable_mib"] == memtable_mib) &
                      (df["cache_mib"] == cache_mib) &
                      (df["dist"] == distribution) &
                      (df["update_pct"] != 100)]
            ax.plot(
                data["update_pct"],
                data["mops_per_s"] * 1000,
                marker="o",
                markersize=5,
                label="{} W {} / C {}"
                    .format(DB_FORMAT[db], memtable_mib, cache_mib),
            )
    if show_legend:
        ax.legend(
            edgecolor="#000000",
            fancybox=False,
            framealpha=1,
        )
    ax.set_ylabel("Throughput (kops/s)")
    ax.set_xlabel("Write Percentage (%)")
    #ax.set_ylim(0, 150)
    return fig, ax


def main():
    out_dir = cond.get_output_path()
    df = pd.read_csv(cond.get_deps_paths()[0] / "all_results.csv")
    for distribution in DISTRIBUTIONS:
        fig, ax = plot_e2e(
            df,
            distribution,
            show_legend=(distribution == DISTRIBUTIONS[0]))
        fig.savefig(out_dir / "e2e-{}.pdf".format(distribution), format="pdf")


if __name__ == "__main__":
    main()
