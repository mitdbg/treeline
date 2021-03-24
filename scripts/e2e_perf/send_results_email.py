import argparse
import pathlib
import smtplib
import socket
from collections import namedtuple
from datetime import datetime
from email.mime.text import MIMEText
from typing import Optional

import pandas as pd

SMTP_SERVER = "outgoing.csail.mit.edu"
SMTP_PORT = 587

SENDER = "llsm-dev@lists.csail.mit.edu"
RECIPIENT = "llsm-dev@lists.csail.mit.edu"

SUBJECT_TEMPLATE = "LLSM Performance Results for {hash}"
MESSAGE_TEMPLATE = """\
<html>
<body>
<p>
The results in this email are from experiments executed against commit {hash} on {machine_name}.
The experiments ran and completed on {date} at {time} (server time).
</p>

{results_table_html}

<p>{comparison_message}</p>

<p>
The "speedup_over_rocksdb" column is LLSM's speedup relative to the same
benchmark executed against RocksDB for the "mops_per_s" column.
</p>

<p>
For an explanation of the benchmarks in this email, please see the wiki page here:
https://dev.azure.com/msr-dsail/LearnedLSM/_wiki/wikis/LearnedLSM.wiki/3/Benchmarks
</p>

<p>This is an automatically generated email.</p>
</body>
</html>"""

COMPARISON_TEMPLATE = """\
The "change_over_prev" column is the "mops_per_s" speedup relative to results from
commit {relative_hash} (experiments completed on {date} at {time}). A value
greater than 1 means that the performance improved."""

NO_COMPARSION_TEMPLATE = """\
The "change_over_prev" column is empty because the experiment runner did not
find any earlier LLSM results to compare against."""

RESULT_TABLE_STYLES = [
    # Spacing between table cells
    {"selector": "td, th", "props": [("padding", "5px 15px 0 0"), ("text-align", "left")]},
    # Adds a border under the table's header
    {"selector": "thead th", "props": [("border-bottom", "1px solid black")]},
]

ResultMetadata = namedtuple("ResultMetadata", ["commit_hash", "timestamp"])


def find_newest_old_result(
    results_dir: pathlib.Path, current_result_meta: ResultMetadata
):
    """
    Searches the results directory for the newest existing result (if any) that (i)
    was from a different commit hash, and (ii) is older than the current result.
    """
    newest_timestamp = 0
    to_return = None

    for result_file in results_dir.iterdir():
        if result_file.suffix != ".csv":
            # Not a result file.
            continue
        meta = parse_result_file_name(result_file)
        if meta.commit_hash == current_result_meta.commit_hash:
            # Result is for the same commit hash
            continue
        if meta.timestamp >= current_result_meta.timestamp:
            # Result is newer than the current result
            continue
        if meta.timestamp <= newest_timestamp:
            # Result is older than the newest one we saw so far.
            continue

        newest_timestamp = meta.timestamp
        to_return = result_file

    return to_return


def parse_result_file_name(file_path: pathlib.Path):
    commit_hash, timestamp_str = file_path.stem.split("_")
    return ResultMetadata(commit_hash, int(timestamp_str))


def compute_changes(
    new_results: pd.DataFrame, old_results: Optional[pd.DataFrame] = None
):
    """
    Joins the new results with the old results (if any) and computes the
    performance changes.
    """
    if old_results is None:
        old_results = pd.DataFrame(columns=new_results.columns)
    joined = pd.merge(
        new_results,
        old_results[["benchmark_name", "mops_per_s"]],
        on="benchmark_name",
        how="left",
        suffixes=["_new", "_old"],
    )
    joined["change_over_prev"] = joined["mops_per_s_new"] / joined["mops_per_s_old"]
    relevant = joined[[
        "benchmark_name",
        "mops_per_s_new",
        "mib_per_s",
        "change_over_prev",
        "speedup_over_rocksdb",
    ]]
    return relevant.rename(
        columns={"mops_per_s_new": "mops_per_s"}
    )


def format_speedups(value):
    if value < 0.95:
        return "color: red"
    elif value > 1.05:
        return "color: green"
    else:
        return "color: black"


def construct_message(
    final_results: pd.DataFrame,
    result_meta: ResultMetadata,
    against_meta: Optional[ResultMetadata],
):
    machine_name = socket.gethostname()

    if against_meta is not None:
        against_time = datetime.fromtimestamp(against_meta.timestamp)
        comparison_message = COMPARISON_TEMPLATE.format(
            relative_hash=against_meta.commit_hash,
            date=against_time.strftime("%B %d, %Y"),
            time=against_time.strftime("%H:%M:%S"),
        )
    else:
        comparison_message = NO_COMPARSION_TEMPLATE

    completion_time = datetime.fromtimestamp(result_meta.timestamp)

    results_table_html = (
        final_results.style
            .applymap(
                format_speedups,
                subset=pd.IndexSlice[
                    :,
                    ["change_over_prev", "speedup_over_rocksdb"],
                ],
            )
            .set_na_rep("--")
            .set_precision(3)
            .set_table_styles(RESULT_TABLE_STYLES)
            .hide_index()
            .render()
    )

    return MESSAGE_TEMPLATE.format(
        machine_name=machine_name,
        hash=result_meta.commit_hash,
        comparison_message=comparison_message,
        results_table_html=results_table_html,
        date=completion_time.strftime("%B %d, %Y"),
        time=completion_time.strftime("%H:%M:%S"),
    )


def send_email(subject: str, html_message: str):
    message = MIMEText(html_message, "html")
    message["From"] = SENDER
    message["To"] = RECIPIENT
    message["Subject"] = subject

    with smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT) as server:
        server.sendmail(SENDER, RECIPIENT, message.as_string())


def main():
    parser = argparse.ArgumentParser(
        description="Process LLSM automated experiment results and send an "
        "email notification with the results."
    )
    parser.add_argument(
        "--result", type=str, required=True, help="Path to the results file."
    )
    parser.add_argument(
        "--compare-against",
        type=str,
        help="Path to the results file to compare against (if any). If no "
        "file is provided, the script will attempt to find a suitable "
        "results comparison file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the email message instead of actually sending it.",
    )
    args = parser.parse_args()

    result_file = pathlib.Path(args.result)
    result_meta = parse_result_file_name(result_file)
    result_df = pd.read_csv(result_file)

    if args.compare_against is not None:
        against_file = pathlib.Path(args.compare_against)
    else:
        against_file = find_newest_old_result(result_file.parent, result_meta)

    if against_file is not None:
        against_df = pd.read_csv(against_file)
        against_meta = parse_result_file_name(against_file)
    else:
        against_df = None
        against_meta = None

    final_results = compute_changes(result_df, against_df)
    html_message = construct_message(final_results, result_meta, against_meta)
    subject = SUBJECT_TEMPLATE.format(hash=result_meta.commit_hash)

    if args.dry_run:
        print("Subject:", subject)
        print(html_message)
    else:
        send_email(
            subject=subject,
            html_message=html_message,
        )


if __name__ == "__main__":
    main()
