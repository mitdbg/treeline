import argparse
import conductor.lib as cond
import pathlib

ZIPFIAN_DIST = """\
      type: zipfian
      theta: 0.99\
"""

UNIFORM_DIST = """\
      type: uniform\
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gen_template", type=str, required=True)
    parser.add_argument("--gen_num_requests", type=int, required=True)
    parser.add_argument("--gen_range_min", type=int, required=True)
    parser.add_argument("--gen_range_max", type=int, required=True)
    parser.add_argument(
        "--gen_distribution", choices=["zipfian", "uniform"], default="zipfian"
    )
    args, unknown = parser.parse_known_args()

    # Load the template.
    template_path = pathlib.Path(args.gen_template)
    with open(template_path, "r") as template_file:
        template = template_file.read()

    # If the template does not contain a key in the config, the config value is
    # just ignored.
    config = {
        "range_min": args.gen_range_min,
        "range_max": args.gen_range_max,
        "num_requests": args.gen_num_requests,
        "distribution": ZIPFIAN_DIST
        if args.gen_distribution == "zipfian"
        else UNIFORM_DIST,
    }

    workload = template.format(**config)

    # Write out the generated workload.
    out_dir = cond.get_output_path()
    with open(out_dir / "workload.yml", "w") as file:
        file.write(workload)

    # Pass through unknown arguments.
    print(" ".join(unknown))


if __name__ == "__main__":
    main()
