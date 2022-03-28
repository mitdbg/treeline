import json
import shutil

import conductor.lib as cond
import pandas as pd


def process_iostat(iostat, device):
    stats = iostat["sysstat"]["hosts"][0]["statistics"]
    read_kb = 0
    written_kb = 0
    for entry in stats:
        disk_stats = entry["disk"]
        for disk in disk_stats:
            if disk["disk_device"] != device:
                continue
            read_kb += disk["kB_read"]
            written_kb += disk["kB_wrtn"]
    return read_kb, written_kb


def main():
    deps = cond.get_deps_paths()
    out_dir = cond.get_output_path()

    all_results = []

    # Make directory for storing the raw iostat traces.
    iostat_dir = out_dir / "iostat"
    iostat_dir.mkdir(exist_ok=True)

    for dep in deps:
        for exp_inst in dep.iterdir():
            # e.g.: rw_sweep-llsm-1024-10-zipfian
            exp_parts = exp_inst.name.split("-")
            record_size_bytes = int(exp_parts[2])
            update_pct = int(exp_parts[3])
            dist = exp_parts[4]
            exp_name = "-".join(exp_parts[1:])

            # Experiment results
            df = pd.read_csv(exp_inst / "results.csv")
            df.pop("read_mib_per_s")
            df.pop("write_mib_per_s")
            df.insert(1, "dist", dist)
            df.insert(2, "record_size_bytes", record_size_bytes)
            df.insert(3, "update_pct", update_pct)

            # Physical data read/written
            with open(exp_inst / "iostat.json", "r") as file:
                iostat = json.load(file)
            read_kb, written_kb = process_iostat(iostat, "nvme0n1")
            df["phys_read_kb"] = read_kb
            df["phys_written_kb"] = written_kb

            all_results.append(df)

            # Make a copy of the iostat trace.
            shutil.copy2(
                exp_inst / "iostat.json", iostat_dir / "{}.json".format(exp_name)
            )

    combined = pd.concat(all_results)
    combined.sort_values(
        ["db", "dist", "record_size_bytes", "update_pct"],
        inplace=True,
        ignore_index=True,
    )
    combined.to_csv(out_dir / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
