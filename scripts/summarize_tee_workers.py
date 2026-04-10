#!/usr/bin/env python3

import argparse
import csv
import glob
import os


def load_rows(case_dir, app_name):
    pattern = os.path.join(case_dir, f"{app_name}_tee_worker_*.tsv")
    paths = sorted(glob.glob(pattern))
    if not paths:
        raise FileNotFoundError(
            f"No tee worker files found for {app_name!r} under {case_dir!r}"
        )

    rows = []
    for path in paths:
        with open(path, "r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile, delimiter="\t")
            for row in reader:
                rows.append(
                    {
                        "fragment_id": int(row["fragment_id"]),
                        "total_tee_time_ms": float(row["total_tee_time_ms"]),
                        "total_tee_batches": int(row["total_tee_batches"]),
                        "total_secure_items": int(row["total_secure_items"]),
                    }
                )
    rows.sort(key=lambda item: item["fragment_id"])
    return rows


def write_summary(rows, output_path):
    max_row = max(rows, key=lambda item: item["total_tee_time_ms"])
    with open(output_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, delimiter="\t", lineterminator="\n")
        writer.writerow(["metric", "value"])
        writer.writerow(["worker_count", len(rows)])
        writer.writerow(["max_worker_fragment_id", max_row["fragment_id"]])
        writer.writerow(["max_total_tee_time_ms", max_row["total_tee_time_ms"]])
        writer.writerow(
            ["sum_total_tee_time_ms", sum(item["total_tee_time_ms"] for item in rows)]
        )
        writer.writerow(
            ["sum_total_tee_batches", sum(item["total_tee_batches"] for item in rows)]
        )
        writer.writerow(
            ["sum_total_secure_items", sum(item["total_secure_items"] for item in rows)]
        )


def main():
    parser = argparse.ArgumentParser(
        description="Summarize per-worker tee metrics and extract the max worker time."
    )
    parser.add_argument("--case-dir", required=True, help="Output directory of one run.")
    parser.add_argument("--app", required=True, help="Application name prefix.")
    parser.add_argument(
        "--summary",
        default="",
        help="Optional summary output path. Defaults to <case-dir>/<app>_tee_summary.tsv",
    )
    args = parser.parse_args()

    rows = load_rows(args.case_dir, args.app)
    summary_path = (
        args.summary
        if args.summary
        else os.path.join(args.case_dir, f"{args.app}_tee_summary.tsv")
    )
    write_summary(rows, summary_path)

    max_row = max(rows, key=lambda item: item["total_tee_time_ms"])
    print(f"worker_count={len(rows)}")
    print(f"max_worker_fragment_id={max_row['fragment_id']}")
    print(f"max_total_tee_time_ms={max_row['total_tee_time_ms']}")


if __name__ == "__main__":
    main()
