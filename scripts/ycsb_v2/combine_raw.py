import argparse
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
    if db == "pg_llsm":
        # To handle the name change.
        return parse_space_file(file_path, "pg_tl")
    raise RuntimeError("Failed to parse space usage for {}:".format(db), str(file_path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--for-factor", action="store_true")
    parser.add_argument("--for-prefetch", action="store_true")
    args = parser.parse_args()
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
            df = pd.read_csv(exp_inst / "results.csv")
            orig_columns = list(df.columns)

            if args.for_factor:
                # The factor analysis experiments have a different name format.
                name = exp_inst.name
                db = "pg_llsm"

                if "-a-" in name:
                    workload = "a"
                elif "-b-" in name:
                    workload = "b"
                elif "-c-" in name:
                    workload = "c"
                elif "-d-" in name:
                    workload = "d"
                elif "-e-" in name:
                    workload = "e"
                else:
                    workload = "f"

                if "nogrp_nocache" in name:
                    order = "nogrp_nocache"
                elif "nogrp" in name:
                    order = "nogrp"
                else:
                    order = "normal"

                df.insert(0, "workload", workload)
                df.insert(1, "order", order)
                df.insert(2, "name", name)

            elif args.for_prefetch:
                # The prefetch experiments also have a different name format.
                # e.g.: prefetch-amzn-1024B-base-1
                # e.g.: prefetch-amzn-1024B-prefetch-1-256
                exp_parts = exp_inst.name.split("-")
                dataset = exp_parts[1]
                db = "pg_llsm"
                config = exp_parts[2]
                dist = "uniform"
                threads = int(exp_parts[4])
                variant = exp_parts[3]
                if len(exp_parts) > 5:
                    bg_threads = int(exp_parts[5])
                else:
                    # Hardcoded in the experiments.
                    bg_threads = 4

                if variant == "base":
                    order = 1
                elif variant == "prefetch":
                    order = 2
                elif variant == "grouping":
                    order = 3
                elif variant == "grouping_prefetch":
                    order = 4
                else:
                    raise RuntimeError

                # Process end-to-end results
                df.insert(0, "dataset", dataset)
                df.insert(1, "config", config)
                df.insert(2, "dist", dist)
                df.insert(3, "threads", threads)
                df.insert(4, "bg_threads", bg_threads)
                df.insert(5, "variant", variant)
                df.insert(6, "order", order)

            else:
                # e.g.: synth-pg_llsm-64B-a-zipfian-1
                # NOTE: We don't need to extract the DB because it is already
                #       recorded in the result CSV.
                exp_parts = exp_inst.name.split("-")
                dataset = exp_parts[0]
                db = exp_parts[1]
                config = exp_parts[2]
                workload = exp_parts[3]
                dist = exp_parts[4]
                threads = int(exp_parts[5])

                # Process end-to-end results
                orig_columns = list(df.columns)
                df.insert(0, "dataset", dataset)
                df.insert(1, "config", config)
                df.insert(2, "dist", dist)
                df.insert(3, "workload", workload)
                df.insert(4, "threads", threads)

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

            # Copy over any additional relevant data and options
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
            elif db == "pg_llsm":
                # Page-grouped LLSM does not have a log, but it writes out counters.
                shutil.copy2(
                    exp_inst / "counters.csv",
                    logs_dir / (exp_inst.name + "_counters.csv"),
                )
            else:
                print("WARNING: Unknown DB", db)

    # Write out the combined results
    combined = pd.concat(all_results)
    orig_columns.remove("db")
    if args.for_factor:
        combined.sort_values(
            ["workload", "order"],
            inplace=True,
            ignore_index=True,
        )
        combined = combined[
            [
                "workload",
                "order",
                "name",
                *orig_columns,
                "phys_read_kb",
                "phys_written_kb",
                "disk_usage_bytes",
            ]
        ]
    elif args.for_prefetch:
        combined.sort_values(
            ["dataset", "config", "dist", "db", "threads", "bg_threads", "order"],
            inplace=True,
            ignore_index=True,
        )
        combined = combined[
            [
                "dataset",
                "config",
                "dist",
                "db",
                "threads",
                "bg_threads",
                "variant",
                *orig_columns,
                "phys_read_kb",
                "phys_written_kb",
                "disk_usage_bytes",
            ]
        ]
    else:
        combined.sort_values(
            ["dataset", "config", "dist", "db", "workload", "threads"],
            inplace=True,
            ignore_index=True,
        )
        combined = combined[
            [
                "dataset",
                "config",
                "dist",
                "db",
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
