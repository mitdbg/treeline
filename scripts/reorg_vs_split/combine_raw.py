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


def extract_counter(counters, name):
    return counters[counters["name"] == name]["value"].item()


def parse_reorg_counters(counter_file):
    counters = pd.read_csv(counter_file)
    overflows = extract_counter(counters, "overflows_created")
    rw_inputs = extract_counter(counters, "rewrite_input_pages")
    rw_outputs = extract_counter(counters, "rewrite_output_pages")
    total_written = overflows + rw_outputs
    total_read = rw_inputs
    return total_read, total_written


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
            df = pd.read_csv(exp_inst / "results.csv")
            df.pop("read_mib_per_s")
            df.pop("write_mib_per_s")
            orig_columns = list(df.columns)

            # e.g.: linear-pages-64B
            # e.g.: linear-grouping-64B-1
            parts = exp_inst.name.split("-")
            dataset = parts[0]
            variant = parts[1]
            if variant.endswith("20m"):
                variant = variant[:-3]
            config = parts[2]
            if len(parts) > 3:
                search_radius = int(parts[3])
            else:
                search_radius = 0

            if variant == "grouping":
                order = 1
            elif variant == "pages":
                order = 2
            else:
                raise AssertionError

            df.insert(0, "dataset", dataset)
            df.insert(1, "config", config)
            df.insert(2, "variant", variant)
            df.insert(3, "order", order)
            df.insert(4, "search_radius", search_radius)

            # Process iostat results (physical I/O)
            read_kb, written_kb = process_iostat(
                iostat_file=exp_inst / "iostat.json",
                device="nvme0n1",
                trace_out_file=iostat_dir / (exp_inst.name + ".csv"),
            )
            reorg_read_pages, reorg_write_pages = parse_reorg_counters(
                exp_inst / "counters.csv"
            )

            df["phys_read_kb"] = read_kb
            df["phys_written_kb"] = written_kb
            df["disk_usage_bytes"] = parse_space_file(
                exp_inst / "db_space.log", "pg_tl"
            )
            df["reorg_read_pages"] = reorg_read_pages
            df["reorg_write_pages"] = reorg_write_pages

            all_results.append(df)

            # Copy over any additional relevant data and options
            shutil.copy2(
                exp_inst / "options.json", options_dir / (exp_inst.name + ".json")
            )
            # Page-grouped LLSM does not have a log, but it writes out counters.
            shutil.copy2(
                exp_inst / "counters.csv",
                logs_dir / (exp_inst.name + "_counters.csv"),
            )

    # Write out the combined results
    combined = pd.concat(all_results)
    orig_columns.remove("db")
    combined.sort_values(
        ["dataset", "config", "order", "search_radius"],
        inplace=True,
        ignore_index=True,
    )
    combined = combined[
        [
            "dataset",
            "config",
            "variant",
            "search_radius",
            *orig_columns,
            "phys_read_kb",
            "phys_written_kb",
            "disk_usage_bytes",
            "reorg_read_pages",
            "reorg_write_pages",
        ]
    ]
    combined.to_csv(out_dir / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
