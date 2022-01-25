import pathlib
import json
import pandas as pd
import conductor.lib as cond


def aggregate_bw(df):
    results = (
        df.groupby(["punits"])
        .agg({"bw_bytes": "sum", "lat_ns": "median"})
        .reset_index()
    )
    results["bw_mib"] = results["bw_bytes"] / 1024 / 1024
    results["lat_us"] = results["lat_ns"] / 1000
    return results


def process_read(raw_json):
    punits = []
    bw_bytes = []
    lat_ns = []
    for job in raw_json["jobs"]:
        name = job["jobname"]
        parts = name.split("-")
        punits.append(int(parts[-1]))
        bw_bytes.append(int(job["read"]["bw_bytes"]))
        lat_ns.append(int(job["read"]["lat_ns"]["mean"]))

    df = pd.DataFrame({"punits": punits, "bw_bytes": bw_bytes, "lat_ns": lat_ns})
    return df


def load_and_process(path):
    with open(pathlib.Path(path) / "fio.json", "r") as file:
        raw_json = json.load(file)

    return aggregate_bw(process_read(raw_json))


def main():
    deps = cond.get_deps_paths()
    assert len(deps) == 1
    processed_data = load_and_process(deps[0])
    out_dir = cond.get_output_path()
    processed_data.to_csv(out_dir / "fio.csv", index=False)


if __name__ == "__main__":
    main()
