import argparse
import csv
import pathlib
import ycsbr_py as ycsbr
from itertools import product

from utils.dataset import extract_keys
from utils.greedy_cache import GreedyCacheDB
from utils.lru_record_cache import LRUCacheDB
from utils.run import run_workload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config")
    parser.add_argument("--record_size_bytes", type=int, default=64)
    parser.add_argument("--cache_size_mib", type=int, default=407)
    parser.add_argument("--records_per_page", type=int, default=50)
    parser.add_argument("--log_writes_period", type=int, default=10000)
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
        writer.writerow(
            [
                "policy",
                "hit_rate",
                "read_ios",
                "write_ios",
                "logged_record_count",
                "num_evictions",
            ]
        )

        for db_type, admit_page in product([GreedyCacheDB, LRUCacheDB], [False, True]):
            db = db_type(
                keys,
                args.records_per_page,
                records_in_cache,
                admit_read_pages=admit_page,
                log_writes_period=args.log_writes_period,
            )
            policy_name = db_type.Name
            run_workload(workload, db)
            db.log_writes()
            policy = "{}_{}".format(
                policy_name, "admit_record" if not admit_page else "admit_page"
            )
            writer.writerow(
                [
                    policy,
                    db.hit_rate,
                    db.read_ios,
                    db.write_ios,
                    db.logged_record_count,
                    db.num_evictions,
                ]
            )


if __name__ == "__main__":
    main()
