import pandas as pd
import conductor.lib as cond


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    results = []

    for dep in deps:
        for exp in dep.iterdir():
            parts = exp.name.split("-")
            workload_parts = parts[1].split("_")
            skew = workload_parts[1]
            theta = int(workload_parts[2]) / 100
            record_size_bytes = int(parts[2])
            df = pd.read_csv(exp / "results.csv")
            df.insert(0, "skew", skew)
            df.insert(1, "theta", theta)
            df.insert(2, "record_size_bytes", record_size_bytes)
            results.append(df)

    combined = pd.concat(results, ignore_index=True)
    combined = combined.sort_values(by=["skew", "theta", "record_size_bytes", "policy"])
    combined.to_csv(out_dir / "results.csv", index=False)


if __name__ == "__main__":
    main()
