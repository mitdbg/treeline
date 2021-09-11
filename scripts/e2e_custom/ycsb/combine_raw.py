import csv
import json
import shutil
import os

import conductor.lib as cond
import pandas as pd


def process_iostat(iostat_file, device, trace_out_file):
    with open(iostat_file, "r") as iostatf:
        iostat = json.load(iostatf)

    stats = iostat["sysstat"]["hosts"][0]["statistics"]
    read_kb = 0
    written_kb = 0
    with open(trace_out_file, "w") as traceoutf:
        trace_out = csv.writer(traceoutf)
        trace_out.writerow(["read_kb", "write_kb"])
        for entry in stats:
            disk_stats = entry["disk"]
            for disk in disk_stats:
                if disk["disk_device"] != device:
                    continue
                read_kb += disk["kB_read"]
                written_kb += disk["kB_wrtn"]
                trace_out.writerow([disk["kB_read"], disk["kB_wrtn"]])

    return read_kb, written_kb


def parse_space_file(file_path, db):
    with open(file_path, "r") as file:
        for line in file:
            line = line.rstrip(os.linesep)
            if not line.endswith(db):
                continue
            return int(line.split("\t")[0])
    raise RuntimeError("Failed to parse space usage:", str(file_path))


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    iostat_dir = out_dir / "iostat"
    options_dir = out_dir / "options"
    logs_dir = out_dir / "logs"

    iostat_dir.mkdir(exist_ok=True)
    options_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    all_results = []

    # Process experiment result configs
    for dep in deps:
        for exp_inst in dep.iterdir():
            # e.g.: ycsb-synthetic-64-llsm-a-1
            # NOTE: We don't need to extract the DB because it is already recorded
            #       in the result CSV.
            exp_parts = exp_inst.name.split("-")
            config = "-".join(exp_parts[:3])
            db = exp_parts[3]
            workload = exp_parts[4]
            threads = int(exp_parts[5])

            # Process end-to-end results
            df = pd.read_csv(exp_inst / "results.csv")
            df.pop("read_mib_per_s")
            df.pop("write_mib_per_s")
            orig_columns = list(df.columns)
            df["config"] = config
            df["workload"] = workload
            df["threads"] = threads

            # Process iostat results (physical I/O)
            read_kb, written_kb = process_iostat(
                iostat_file=exp_inst / "iostat.json",
                device="nvme0n1",
                trace_out_file=iostat_dir / (exp_inst.name + ".csv"),
            )
            df["phys_read_kb"] = read_kb
            df["phys_written_kb"] = written_kb
            df["disk_usage_bytes"] = parse_space_file(exp_inst / "db_space.log", db)

            all_results.append(df)

            # Copy over any relevant logs and options
            shutil.copy2(
                exp_inst / "options.json", options_dir / (exp_inst.name + ".json")
            )
            if db == "rocksdb":
                shutil.copy2(
                    exp_inst / "rocksdb.log", logs_dir / (exp_inst.name + ".log")
                )
            elif db == "llsm":
                shutil.copy2(exp_inst / "llsm.log", logs_dir / (exp_inst.name + ".log"))
            elif db == "leanstore":
                # As far as we know, LeanStore does not log debug information to
                # a separate file.
                pass
            else:
                print("WARNING: Unknown DB", db)

    # Write out the combined results
    combined = pd.concat(all_results)
    combined.sort_values(
        ["config", "workload", "db", "threads"],
        inplace=True,
        ignore_index=True,
    )
    combined = combined[
        [
            "config",
            "workload",
            "threads",
            *orig_columns,
            "phys_read_kb",
            "phys_written_kb",
            "disk_usage_bytes",
        ]
    ]
    combined.to_csv(out_dir / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
