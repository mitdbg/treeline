import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

from plot_common import DATASET_COLORS, DATASET_MAP, DATASET_LINESTYLE

plt.rcParams["font.size"] = 14


def plot_seg_efficiency(data, vary, show_ylabel=False, show_legend=False):
    datasets = data["dataset"].unique()
    if show_ylabel:
        fig, ax = plt.subplots(figsize=(3.35, 2.5), tight_layout=True)
    else:
        fig, ax = plt.subplots(figsize=(3, 2.5), tight_layout=True)

    for dataset in datasets:
        rel = data[data["dataset"] == dataset]
        ax.plot(
            rel[vary],
            rel["in_segment_fraction"] * 100,
            label=DATASET_MAP[dataset],
            linewidth=3,
            color=DATASET_COLORS[dataset],
            linestyle=DATASET_LINESTYLE[dataset],
        )
    if show_legend:
        ax.legend(
            fancybox=False,
            edgecolor="#000",
            fontsize="small",
            loc="lower right",
            bbox_to_anchor=(1.02, -0.02),
        )
    ax.set_xlabel("Epsilon" if vary == "delta" else vary.capitalize())
    if show_ylabel:
        ax.set_ylabel("Seg. Eff. (%)")
    return fig


def main():
    deps = cond.get_deps_paths()
    assert len(deps) == 1
    out_dir = cond.get_output_path()
    in_dir = deps[0]

    vary_delta = pd.read_csv(in_dir / "goal_44.csv")
    fig = plot_seg_efficiency(vary_delta, "delta", show_ylabel=True, show_legend=True)
    fig.savefig(out_dir / "goal_44.pdf")
    plt.close(fig)

    vary_goal = pd.read_csv(in_dir / "delta_5.csv")
    fig = plot_seg_efficiency(vary_goal, "goal")
    fig.savefig(out_dir / "delta_5.pdf")
    plt.close(fig)


if __name__ == "__main__":
    main()
