import pandas as pd
import conductor.lib as cond


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    results = []

    for dep in deps:
        for exp in dep.iterdir():
            parts = exp.name.split("-")
            theta = int(parts[2]) / 100
            skew_type = parts[1]
            df = pd.read_csv(exp / "results.csv")
            df.insert(0, "theta", theta)
            df.insert(0, "skew_type", skew_type)
            results.append(df)
    
    combined = pd.concat(results, ignore_index=True)
    combined = combined.sort_values(by=["skew_type", "theta", "flush_fill_threshold"])
    combined.to_csv(out_dir / "results.csv", index=False)


if __name__ == "__main__":
    main()
