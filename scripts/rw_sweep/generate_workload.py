import argparse
import math
import conductor.lib as cond


LOAD_SECTION = """\
record_size_bytes: {record_size_bytes}

load:
  num_records: {num_records}
  distribution:
    type: linspace
    start_key: 1000
    step_size: 1000

"""

ZIPFIAN = """\
    distribution:
      type: zipfian
      theta: 0.79\
"""

UNIFORM = """\
    distribution:
      type: uniform\
"""

RUN_SECTION = """\
run:
- num_requests: {num_requests}
  read:
    proportion_pct: {read_pct}
{distribution}
  scan:
    proportion_pct: {scan_pct}
    max_length: 100
{distribution}
  update:
    proportion_pct: {update_pct}
{distribution}
"""

PRELOAD_RUN = """\
run:
- num_requests: {num_requests}
  update:
    proportion_pct: 100
    distribution:
      type: uniform
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gen_record_size_bytes", type=int, required=True)
    parser.add_argument("--gen_num_records", type=int, required=True)
    parser.add_argument("--gen_num_requests", type=int, required=True)
    parser.add_argument("--gen_distribution", choices=["zipfian", "uniform"])
    parser.add_argument("--gen_update_percent", type=int)
    # What percent of the reads should be scans? This is applied to the
    # workload's read percentage.
    #
    # For example, suppose 10% of the workload should consist of reads and this
    # argument is set to 5%. Then 1% of the workload will consist of scans
    # whereas the other 9% will be point reads (we round fractional percentages
    # up to the nearest percent).
    parser.add_argument("--gen_scan_read_percent", type=int)
    parser.add_argument("--gen_for_preload", action="store_true")
    args, unknown = parser.parse_known_args()

    load_section = LOAD_SECTION.format(
        record_size_bytes=args.gen_record_size_bytes,
        num_records=args.gen_num_records,
    )

    if args.gen_for_preload:
        workload = "".join(
            [load_section, PRELOAD_RUN.format(num_requests=args.gen_num_requests // 2)]
        )
    else:
        total_read_pct = 100 - args.gen_update_percent
        scan_pct = math.ceil(total_read_pct * (args.gen_scan_read_percent / 100))
        read_pct = total_read_pct - scan_pct
        assert read_pct + scan_pct + args.gen_update_percent == 100

        run_section = RUN_SECTION.format(
            num_requests=args.gen_num_requests,
            read_pct=read_pct,
            scan_pct=scan_pct,
            update_pct=args.gen_update_percent,
            distribution=ZIPFIAN if args.gen_distribution == "zipfian" else UNIFORM,
        )
        workload = "".join([load_section, run_section])

    # Write out the generated workload
    out_dir = cond.get_output_path()
    with open(out_dir / "workload.yml", "w") as file:
        file.write(workload)

    # Pass through unknown arguments
    print(" ".join(unknown))


if __name__ == "__main__":
    main()
