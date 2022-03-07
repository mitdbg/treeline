import argparse
import conductor.lib as cond
import matplotlib.pyplot as plt
import numpy as np

from plot_common import COLORS

plt.rcParams["font.size"] = 14


def load_dataset(path):
    with open(path) as f:
        keys = [int(line) for line in f.readlines()]
    keys.sort()
    return keys


def plot_cdf(dataset, inset_slice=None, show_ylabel=False):
    y = np.arange(len(dataset)) / (len(dataset) - 1)
    x = np.array(dataset, dtype=np.float64)
    max_key = np.max(x)
    norm_x = x / max_key
    if show_ylabel:
        figsize = (2.4, 2.2)
    else:
        figsize = (2.1, 2.2)
    fig, ax = plt.subplots(figsize=figsize, tight_layout=True)
    ax.plot(
        norm_x,
        y,
        linewidth=2.5,
        color=COLORS["pg_llsm"],
    )
    ax.set_xlabel("Norm. Key")
    if show_ylabel:
        ax.set_ylabel("Norm. Pos.")

    if inset_slice is not None:
        axins = ax.inset_axes([0.45, 0.1, 0.5, 0.35])
        axins.plot(
            norm_x[inset_slice[0] : inset_slice[1]],
            y[inset_slice[0] : inset_slice[1]],
            linewidth=2,
            color=COLORS["pg_llsm"],
        )
        axins.set_xticks([])
        axins.set_yticks([])
        ax.indicate_inset_zoom(axins)

    return fig


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_path", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--show_ylabel", action="store_true")
    parser.add_argument("--inset_min", type=int)
    parser.add_argument("--inset_max", type=int)
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    if args.out_dir is not None:
        out_dir = args.out_dir
    else:
        out_dir = cond.get_output_path()

    dataset = load_dataset(args.dataset_path)
    if args.inset_min is not None and args.inset_max is not None:
        inset = (args.inset_min, args.inset_max)
    else:
        inset = None
    fig = plot_cdf(dataset, inset_slice=inset, show_ylabel=args.show_ylabel)
    fig.savefig(out_dir / "{}.pdf".format(args.name))
    plt.close(fig)


if __name__ == "__main__":
    main()
