import argparse
import csv
import sys
import ycsbr_py as ycsbr

from utils.dataset import extract_keys
from utils.io_count import IOCounter
from utils.run import run_workload


def get_cluster_by_access_mapper(dataset, access_freqs, records_per_page):
    key_access_counts = []
    for key, access_freq in access_freqs.items():
        key_access_counts.append((key, access_freq))
    for key in dataset:
        if key in access_freqs:
            continue
        key_access_counts.append((key, 0))

    # Largest access frequency first
    key_access_counts.sort(key=lambda data: -data[1])

    # Cluster by access frequency.
    # N.B. This mapping is as large as the dataset.
    key_to_page_map = {}
    curr_page = 0
    page_count = 0
    for key, _ in key_access_counts:
        page_count += 1
        key_to_page_map[key] = curr_page
        if page_count >= records_per_page:
            curr_page += 1
            page_count = 0

    def page_mapper(key):
        return key_to_page_map[key]

    return page_mapper, key_access_counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("workload_config")
    parser.add_argument("--record_size_bytes", type=int, default=64)
    parser.add_argument("--page_fill_pct", type=int, default=50)
    parser.add_argument("--page_size_bytes", type=int, default=4096)
    parser.add_argument("--cache_size_mib", type=int, default=407)
    parser.add_argument("--print_csv", action="store_true")
    parser.add_argument("--record_accesses", type=str)
    args = parser.parse_args()

    cache_capacity = int(args.cache_size_mib * 1024 * 1024 / args.page_size_bytes)
    records_per_page = int(
        args.page_size_bytes * args.page_fill_pct / 100 / args.record_size_bytes
    )

    if not args.print_csv:
        print("Record size bytes: {}".format(args.record_size_bytes))
        print("Page fill: {}%".format(args.page_fill_pct))
        print("Cache size: {} MiB".format(args.cache_size_mib))
        print("Records per page: {}".format(records_per_page))
        print("Pages in cache: {}".format(cache_capacity))

    workload = ycsbr.PhasedWorkload.from_file(
        args.workload_config, set_record_size_bytes=args.record_size_bytes
    )
    keys = extract_keys(workload.get_load_trace())

    key_clustered_db = IOCounter(
        IOCounter.get_key_order_page_mapper(keys, records_per_page),
        cache_capacity,
    )
    run_workload(workload, key_clustered_db)

    if not args.print_csv:
        print()
        print("Key Order Clustering")
        print("Read I/Os:  {}".format(key_clustered_db.read_ios))
        print("Write I/Os: {}".format(key_clustered_db.write_ios))

    access_mapper, access_data = get_cluster_by_access_mapper(
        keys, key_clustered_db.key_access_freqs, records_per_page
    )
    access_clustered_db = IOCounter(access_mapper, cache_capacity)
    run_workload(workload, access_clustered_db)

    if not args.print_csv:
        print()
        print("Access Order Clustering")
        print("Read I/Os:  {}".format(access_clustered_db.read_ios))
        print("Write I/Os: {}".format(access_clustered_db.write_ios))

    if args.print_csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(["cluster_kind", "read_ios", "write_ios"])
        writer.writerow(
            ["key_order", key_clustered_db.read_ios, key_clustered_db.write_ios]
        )
        writer.writerow(
            ["access_freq", access_clustered_db.read_ios, access_clustered_db.write_ios]
        )

    if args.record_accesses is not None:
        with open(args.record_accesses, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["key", "access_count"])
            for key, access in access_data:
                if access == 0:
                    break
                writer.writerow([key, access])


if __name__ == "__main__":
    main()
