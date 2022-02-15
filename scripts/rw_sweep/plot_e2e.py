import argparse

import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams["font.size"] = 14


DISTRIBUTIONS = ["uniform", "zipfian"]
DB_FORMAT = {
    "llsm": "LLSM (ours)",
    "rocksdb": "RocksDB",
}


def plot_e2e(
    df,
    distribution,
    record_size,
    show_legend=False,
    exclude_100=False,
    title=None,
    ylim=None,
):
    fig, ax = plt.subplots(
        figsize=(6.65, 4),
        tight_layout=True,
        frameon=False,
    )
    dbs = df["db"].unique()
    for db in dbs:
        df_filter = (
            (df["db"] == db)
            & (df["dist"] == distribution)
            & (df["record_size_bytes"] == record_size)
        )
        if exclude_100:
            df_filter &= df["update_pct"] != 100

        data = df[df_filter]
        ax.plot(
            data["update_pct"],
            data["krequests_per_s"],
            marker="o",
            markersize=5,
            label=DB_FORMAT[db],
        )
    if show_legend:
        ax.legend(
            edgecolor="#000000",
            fancybox=False,
            framealpha=1,
        )
    ax.set_ylabel("Throughput (kreq/s)")
    ax.set_xlabel("Update Percentage (%)")
    if ylim is not None:
        ax.set_ylim(ylim)
    if title is not None:
        ax.set_title(title)

    if not exclude_100:
        # Print the value at 100.
        rel = df[
            (df["db"] == "rocksdb")
            & (df["dist"] == distribution)
            & (df["record_size_bytes"] == record_size)
            & (df["update_pct"] == 100)
        ]
        max_val = rel["krequests_per_s"].iloc[0]
        fig.text(0.82, 0.94, "{:.2f}".format(max_val))

    return fig, ax


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--distribution", type=str, required=True)
    parser.add_argument("--record_size_bytes", type=int, required=True)
    parser.add_argument("--y_min", type=int)
    parser.add_argument("--y_max", type=int)
    args = parser.parse_args()

    ylim = None
    if args.y_min is not None and args.y_max is not None:
        ylim = (args.y_min, args.y_max)

    out_dir = cond.get_output_path()
    df = pd.read_csv(cond.get_deps_paths()[0] / "all_results.csv")
    fig, _ = plot_e2e(
        df,
        distribution=args.distribution,
        record_size=args.record_size_bytes,
        show_legend=True,
        ylim=ylim,
    )
    fig.savefig(
        out_dir
        / "rw_sweep-{}-{}.pdf".format(args.distribution, args.record_size_bytes),
        format="pdf",
    )


if __name__ == "__main__":
    main()
