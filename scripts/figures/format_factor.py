import conductor.lib as cond
import pandas as pd


def build_table(data, workload):
    df = data[data["workload"] == workload].copy()
    df["read_gib"] = df["phys_read_kb"] / 1024 / 1024
    df["write_gib"] = df["phys_written_kb"] / 1024 / 1024
    df = df[["read_gib", "write_gib", "krequests_per_s"]]
    df["speedup_over_prev"] = (
        df["krequests_per_s"].rolling(2).apply(lambda x: x.iloc[1] / x.iloc[0])
    )
    df.insert(0, "name", ["TreeLine Base", "+ Record Cache", "+ Page Grouping"])
    return df


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()
    results = pd.read_csv(deps[0] / "all_results.csv")

    point = build_table(results, "a")
    scan = build_table(results, "scan_only")
    columns = [
        "name",
        "read_gib",
        "write_gib",
        "krequests_per_s",
        "speedup_over_prev",
    ]

    with open(out_dir / "point.tex", "w") as file:
        point.to_latex(
            buf=file,
            columns=columns,
            na_rep="---",
            float_format="{:.2f}".format,
            index=False,
            header=False,
        )

    with open(out_dir / "scan.tex", "w") as file:
        scan.to_latex(
            buf=file,
            columns=columns,
            na_rep="---",
            float_format="{:.2f}".format,
            index=False,
            header=False,
        )


if __name__ == "__main__":
    main()
