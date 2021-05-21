import shutil

import conductor.lib as cond
import pandas as pd


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    iostat_dir = out_dir / "iostat"
    samples_dir = out_dir / "samples"
    options_dir = out_dir / "options"
    logs_dir = out_dir / "logs"

    iostat_dir.mkdir(exist_ok=True)
    samples_dir.mkdir(exist_ok=True)
    options_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    overall_results = []

    for dep_path in deps:
        if "rocksdb" in dep_path.name:
            # RocksDB results
            shutil.copy2(dep_path / "throughput-0.csv", samples_dir / "rocksdb.csv")
            shutil.copy2(dep_path / "options.json", options_dir / "rocksdb.json")
            shutil.copy2(dep_path / "iostat.json", iostat_dir / "rocksdb.json")
            shutil.copy2(dep_path / "rocksdb.log", logs_dir)
            df = pd.read_csv(dep_path / "results.csv")
            df["config"] = "rocksdb"
            overall_results.append(df)
        else:
            # LLSM results
            for instance in dep_path.iterdir():
                # e.g. 512B-llsm-deferral-True-memory-True-equal-bab-long
                config_parts = instance.name.split("-")
                config = "-".join(config_parts[1:6])
                shutil.copy2(instance / "throughput-0.csv", samples_dir / (config + ".csv"))
                shutil.copy2(instance / "options.json", options_dir / (config + ".json"))
                shutil.copy2(instance / "iostat.json", iostat_dir / (config + ".json"))
                shutil.copy2(instance / "llsm.log", logs_dir / (config + ".log"))
                df = pd.read_csv(instance / "results.csv")
                df["config"] = "-".join(config.split("-")[1:])
                overall_results.append(df)

    # Output the combined results
    pd.concat(overall_results).sort_values(["db", "config"]).to_csv(out_dir / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
