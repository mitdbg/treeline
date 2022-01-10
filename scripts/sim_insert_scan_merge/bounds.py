import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()

    min_val = None
    max_val = None

    with open(args.file) as file:
        for line in file.readlines():
            key = int(line)
            if min_val is None or key < min_val:
                min_val = key
            if max_val is None or key > max_val:
                max_val = key

    print("Min key:", min_val)
    print("Max key:", max_val)


if __name__ == "__main__":
    main()
