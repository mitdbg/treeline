import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams["font.size"] = 14


def plot_samples(
    samples,
    plot_none=False,
    plot_memory=False,
    plot_deferral=False,
    plot_both=False,
    plot_rocksdb=False,
):
    fig, ax = plt.subplots(figsize=(14, 4), tight_layout=True, frameon=False)
    ax.axvspan(50, 100, alpha=0.05, color="black")
    if plot_none:
        ax.plot(
            samples["none"]["mrecords_per_s"] * 1000,
            label="LLSM Static",
            color="tab:blue",
            linewidth=2,
        )
    if plot_memory:
        ax.plot(
            samples["memory"]["mrecords_per_s"] * 1000,
            label="LLSM Memory Tuning Only",
            color="tab:purple",
            linewidth=2,
        )
    if plot_deferral:
        ax.plot(
            samples["deferral"]["mrecords_per_s"] * 1000,
            label="LLSM Deferral Tuning Only",
            color="tab:orange",
            linewidth=2,
        )
    if plot_both:
        ax.plot(
            samples["both"]["mrecords_per_s"] * 1000,
            label="LLSM Auto-Tuned",
            color="tab:red",
            linewidth=2,
        )
    if plot_rocksdb:
        ax.plot(
            samples["rocksdb"]["mrecords_per_s"] * 1000,
            label="RocksDB",
            color="tab:brown",
            linewidth=2,
        )
    ax.legend(
        edgecolor="#000000",
        fancybox=False,
        framealpha=1,
        loc="upper right",
    )
    ax.set_xlabel("Number of Workload Requests Processed (100k)")
    ax.set_ylabel("Throughput (kops/s)")
    ax.text(5, 5, "Phase 1 (Read Heavy)", color="gray")
    ax.text(50.5, 5, "Phase 2 (Balanced Read/Write)", color="gray")
    ax.text(110, 5, "Phase 3 (Read Heavy)", color="gray")
    ax.set_ylim(0, 110)
    ax.set_xlim(-5, 205)
    return fig, ax


def main():
    data_dir = cond.get_deps_paths()[0]
    output_dir = cond.get_output_path()
    samples_dir = data_dir / "samples"

    samples = {
        "none": pd.read_csv(samples_dir / "llsm-deferral-False-memory-False.csv"),
        "memory": pd.read_csv(samples_dir / "llsm-deferral-False-memory-True.csv"),
        "deferral": pd.read_csv(samples_dir / "llsm-deferral-True-memory-False.csv"),
        "both": pd.read_csv(samples_dir / "llsm-deferral-True-memory-True.csv"),
        "rocksdb": pd.read_csv(samples_dir / "rocksdb.csv"),
    }

    # Overall
    fig, _ = plot_samples(samples, plot_none=True, plot_both=True, plot_rocksdb=True)
    fig.savefig(output_dir / "tuning-overall.pdf", format="pdf")
    fig.savefig(output_dir / "tuning-overall.png", format="png")

    # Deferral tuning comparison
    fig, _ = plot_samples(samples, plot_none=True, plot_deferral=True, plot_both=True)
    fig.savefig(output_dir / "tuning-deferral.pdf", format="pdf")
    fig.savefig(output_dir / "tuning-deferral.png", format="png")

    # Memory tuning comparison
    fig, _ = plot_samples(samples, plot_none=True, plot_memory=True, plot_both=True)
    fig.savefig(output_dir / "tuning-memory.pdf", format="pdf")
    fig.savefig(output_dir / "tuning-memory.png", format="png")


if __name__ == "__main__":
    main()
