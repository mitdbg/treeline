import argparse
import csv
import pathlib
import ycsbr_py as ycsbr

from utils.greedy_cache import GreedyCacheDB


def extract_keys(ycsbr_dataset):
    keys = []
    for i in range(len(ycsbr_dataset)):
        keys.append(ycsbr_dataset.get_key_at(i))
    return keys


def run_workload(workload, db):
    session = ycsbr.Session(num_threads=1)
    session.set_database(db)
    session.initialize()
    try:
        session.run_phased_workload(workload)
        return db
    finally:
        session.terminate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config")
    parser.add_argument("--record_size_bytes", type=int, default=64)
    parser.add_argument("--cache_size_mib", type=int, default=407)
    parser.add_argument("--records_per_page", type=int, default=50)
    parser.add_argument("--out_dir", type=str)
    args = parser.parse_args()

    records_in_cache = int(args.cache_size_mib * 1024 * 1024 / args.record_size_bytes)

    workload = ycsbr.PhasedWorkload.from_file(
        args.workload_config, set_record_size_bytes=args.record_size_bytes
    )
    keys = extract_keys(workload.get_load_trace())

    if args.out_dir is not None:
        out_path = pathlib.Path(args.out_dir)
    else:
        import conductor.lib as cond
        out_path = cond.get_output_path()

    with open(out_path / "results.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(["policy", "hit_rate", "read_ios", "write_ios"])

        for admit_page in [False, True]:
            db = GreedyCacheDB(
                keys,
                args.records_per_page,
                records_in_cache,
                admit_read_pages=admit_page,
            )
            run_workload(workload, db)
            policy = "greedy_admit_record" if not admit_page else "greedy_admit_page"
            writer.writerow([policy, db.hit_rate, db.read_ios, db.write_ios])


if __name__ == "__main__":
    main()
