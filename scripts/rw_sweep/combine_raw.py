import json

import conductor.lib as cond
import pandas as pd


def load_and_combine():
    deps = cond.get_deps_paths()
    assert len(deps) == 1

    all_results = []

    for exp_inst in deps[0].iterdir():
        # e.g.: 64B-llsm-0-mem-3200-cache-64-uniform
        exp_parts = exp_inst.name.split("-")
        update_pct = int(exp_parts[2])
        memtable_mib = int(exp_parts[4])
        cache_mib = int(exp_parts[6])
        dist = exp_parts[7]

        # Experiment results
        df = pd.read_csv(exp_inst / "results.csv")
        df.pop("read_mib_per_s")
        df.pop("write_mib_per_s")
        df["update_pct"] = update_pct
        df["memtable_mib"] = memtable_mib
        df["cache_mib"] = cache_mib
        df["dist"] = dist

        # Physical data read/written
        with open(exp_inst / "iostat.json", "r") as file:
            iostat = json.load(file)
        read_kb, written_kb = process_iostat(iostat, "nvme0n1")
        df["phys_read_kb"] = read_kb
        df["phys_written_kb"] = written_kb

        all_results.append(df)
    
    combined = pd.concat(all_results)
    combined.sort_values(
        ["db", "dist", "update_pct", "memtable_mib", "cache_mib"],
        inplace=True,
        ignore_index=True,
    )
    return combined


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
    combined = load_and_combine()
    combined.to_csv(cond.get_output_path() / "all_results.csv", index=False)


if __name__ == "__main__":
    main()
