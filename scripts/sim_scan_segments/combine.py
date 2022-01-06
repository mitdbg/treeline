import pandas as pd
import conductor.lib as cond


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    results = []

    for dep in deps:
        for exp in dep.iterdir():
            parts = exp.name.split("-")
            dataset = parts[0]
            df = pd.read_csv(exp / "results.csv")
            df.insert(0, "dataset", dataset)
            results.append(df)

    combined = pd.concat(results, ignore_index=True)
    combined = combined.sort_values(by=["dataset", "workload", "goal"])
    combined.to_csv(out_dir / "results.csv", index=False)


if __name__ == "__main__":
    main()
