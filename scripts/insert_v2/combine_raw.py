import csv
import json
import math
import shutil
import sys
import os

import conductor.lib as cond
import pandas as pd


def process_iostat(iostat_file, device, trace_out_file):
    if not iostat_file.exists():
        print("WARNING: Did not find", str(iostat_file), file=sys.stderr)
        return math.nan, math.nan 

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
    if not file_path.exists():
        print("WARNING: Did not find", str(file_path), file=sys.stderr)
        return math.nan 

    with open(file_path, "r") as file:
        for line in file:
            line = line.rstrip(os.linesep)
            if not line.endswith(db):
                continue
            return int(line.split("\t")[0])
    raise RuntimeError("Failed to parse space usage for {}:".format(db), str(file_path))


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
            # e.g.: palloc-pg_llsm-64B-taxi-1
            # NOTE: We don't need to extract the DB because it is already
            #       recorded in the result CSV.
            exp_parts = exp_inst.name.split("-")
            db = exp_parts[1]
            config = exp_parts[2]
            workload = exp_parts[3]
            threads = int(exp_parts[4])

            def process_instance(setup):
                df = pd.read_csv(exp_inst / setup / "results.csv")
                df.pop("read_mib_per_s")
                df.pop("write_mib_per_s")

                # Process end-to-end results
                df.insert(0, "workload", workload)
                df.insert(1, "config", config)
                df.insert(2, "threads", threads)
                df.insert(3, "setup", setup)

                # Process iostat results (physical I/O)
                read_kb, written_kb = process_iostat(
                    iostat_file=exp_inst / setup / "iostat.json",
                    device="nvme0n1",
                    trace_out_file=iostat_dir / ("{}_{}.csv".format(exp_inst.name, setup)),
                )
                df["phys_read_kb"] = read_kb
                df["phys_written_kb"] = written_kb
                df["disk_usage_bytes"] = parse_space_file(
                    exp_inst / setup / "db_space.log", db
                )

                all_results.append(df)

                if db == "pg_llsm":
                    # Page-grouped LLSM writes out counters.
                    shutil.copy2(
                        exp_inst / setup / "counters.csv",
                        logs_dir / ("{}_{}_counters.csv".format(exp_inst.name, setup)),
                    )

            process_instance("no_alloc")
            if db == "pg_llsm":
                process_instance("perfect_alloc")

            # Copy over any additional relevant data and options
            shutil.copy2(
                exp_inst / "options.json", options_dir / (exp_inst.name + ".json")
            )

    # Write out the combined results
    combined = pd.concat(all_results)
    all_cols = combined.columns
    config_cols = ["workload", "config", "db", "threads", "setup"]
    combined.sort_values(
        config_cols,
        inplace=True,
        ignore_index=True,
    )
    combined = combined[
        [
            *config_cols,
            *filter(lambda c: c not in config_cols, all_cols),
        ]
    ]
    combined.to_csv(out_dir / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
