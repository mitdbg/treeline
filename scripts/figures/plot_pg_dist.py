import conductor.lib as cond
import matplotlib.pyplot as plt
import pandas as pd

from plot_common import DATASET_COLORS, DATASET_MAP

plt.rcParams["font.size"] = 14


def normalize_data(data):
    norm = data.copy()
    norm["num_pages"] = norm["num_segments"] * norm["segment_page_count"]
    norm["norm"] = norm["num_pages"] / norm["num_pages"].sum()
    return norm


def load_data(data_dir):
    results = {}
    for file in data_dir.iterdir():
        if file.suffix != ".csv":
            continue
        df = pd.read_csv(file)
        norm = normalize_data(df)
        results[file.stem] = norm
    return results


def plot_dist(all_data, config, show_legend=False):
    datasets = ["amzn", "osm", "synth"]
    fig, axs = plt.subplots(1, 3, figsize=(6.5, 2.25), tight_layout=True, sharey=True)

    handles = []

    for dataset, ax in zip(datasets, axs):
        norm = all_data["{}-{}-segs".format(dataset, config)]

        xlabels = norm["segment_page_count"]
        xpos = list(range(len(xlabels)))
        h = ax.bar(xpos, norm["norm"] * 100, color=DATASET_COLORS[dataset])
        handles.append(h)
        ax.set_xticks(xpos, xlabels)
        ax.set_ylim((0, 110))
        ax.set_xlabel("Segment Size")
        if dataset == "amzn":
            ax.set_ylabel("Proportion (%)")
    if show_legend:
        fig.legend(
            handles,
            map(lambda d: DATASET_MAP[d], datasets),
            fancybox=False,
            edgecolor="#000",
            fontsize="small",
            loc="upper left",
            bbox_to_anchor=(0.14, 0.92),
        )

    return fig


def main():
    deps = cond.get_deps_paths()
    assert len(deps) == 1
    in_dir = deps[0]
    out_dir = cond.get_output_path()
    seg_dists = load_data(in_dir / "seg_dist")

    fig = plot_dist(seg_dists, "64B", show_legend=True)
    fig.savefig(out_dir / "pg_dist_64B.pdf")
    plt.close(fig)

    fig = plot_dist(seg_dists, "1024B")
    fig.savefig(out_dir / "pg_dist_1024B.pdf")
    plt.close(fig)


if __name__ == "__main__":
    main()
