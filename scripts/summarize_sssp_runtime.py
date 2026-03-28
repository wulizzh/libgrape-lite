#!/usr/bin/env python3

import argparse
import csv
import glob
import os
from collections import defaultdict


def read_rows(case_dir):
    pattern = os.path.join(case_dir, "sssp_runtime_worker_*.tsv")
    paths = sorted(glob.glob(pattern))
    if not paths:
        raise FileNotFoundError(
            f"No runtime telemetry files found under {case_dir!r}"
        )

    rows = []
    for path in paths:
        with open(path, "r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile, delimiter="\t")
            for row in reader:
                row["fragment_id"] = int(row["fragment_id"])
                row["superstep"] = int(row["superstep"])
                for key in (
                    "duration_ms",
                    "active_vertices",
                    "active_private_vertices",
                    "private_candidates",
                    "next_vertices",
                    "next_outer_vertices",
                ):
                    row[key] = float(row[key]) if key == "duration_ms" else int(row[key])
                for key in (
                    "processed_private_candidates",
                    "deferred_private_candidates",
                    "secure_memory_level",
                    "feedback_triggered",
                    "coscheduling_activated",
                ):
                    row[key] = int(row.get(key, 0) or 0)
                row["feedback_overhead_ms"] = float(
                    row.get("feedback_overhead_ms", 0.0) or 0.0
                )
                rows.append(row)
    rows.sort(key=lambda item: (item["superstep"], item["fragment_id"]))
    return rows


def write_merged(rows, path):
    fieldnames = [
        "fragment_id",
        "superstep",
        "phase",
        "duration_ms",
        "active_vertices",
        "active_private_vertices",
        "private_candidates",
        "next_vertices",
        "next_outer_vertices",
        "processed_private_candidates",
        "deferred_private_candidates",
        "secure_memory_level",
        "feedback_overhead_ms",
        "feedback_triggered",
        "coscheduling_activated",
    ]
    with open(path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_superstep_summary(rows, path):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["superstep"], row["phase"])].append(row)

    fieldnames = [
        "superstep",
        "phase",
        "worker_count",
        "max_duration_ms",
        "avg_duration_ms",
        "imbalance_max_over_avg",
        "max_active_vertices",
        "avg_active_vertices",
        "max_active_private_vertices",
        "avg_active_private_vertices",
        "max_private_candidates",
        "avg_private_candidates",
        "max_next_vertices",
        "avg_next_vertices",
        "max_next_outer_vertices",
        "avg_next_outer_vertices",
        "max_processed_private_candidates",
        "avg_processed_private_candidates",
        "max_deferred_private_candidates",
        "avg_deferred_private_candidates",
        "max_secure_memory_level",
        "avg_secure_memory_level",
        "avg_feedback_overhead_ms",
        "sum_feedback_overhead_ms",
        "feedback_triggered_workers",
        "coscheduling_activated_workers",
    ]

    with open(path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for (superstep, phase) in sorted(grouped):
            items = grouped[(superstep, phase)]
            worker_count = len(items)

            def values(key):
                return [item[key] for item in items]

            duration_values = values("duration_ms")
            active_values = values("active_vertices")
            active_private_values = values("active_private_vertices")
            private_candidate_values = values("private_candidates")
            next_values = values("next_vertices")
            next_outer_values = values("next_outer_vertices")
            processed_private_values = values("processed_private_candidates")
            deferred_private_values = values("deferred_private_candidates")
            secure_memory_values = values("secure_memory_level")
            feedback_overhead_values = values("feedback_overhead_ms")
            feedback_trigger_values = values("feedback_triggered")
            coscheduling_values = values("coscheduling_activated")

            avg_duration = sum(duration_values) / worker_count
            writer.writerow(
                {
                    "superstep": superstep,
                    "phase": phase,
                    "worker_count": worker_count,
                    "max_duration_ms": max(duration_values),
                    "avg_duration_ms": avg_duration,
                    "imbalance_max_over_avg": (
                        max(duration_values) / avg_duration if avg_duration > 0 else 0.0
                    ),
                    "max_active_vertices": max(active_values),
                    "avg_active_vertices": sum(active_values) / worker_count,
                    "max_active_private_vertices": max(active_private_values),
                    "avg_active_private_vertices": sum(active_private_values)
                    / worker_count,
                    "max_private_candidates": max(private_candidate_values),
                    "avg_private_candidates": sum(private_candidate_values) / worker_count,
                    "max_next_vertices": max(next_values),
                    "avg_next_vertices": sum(next_values) / worker_count,
                    "max_next_outer_vertices": max(next_outer_values),
                    "avg_next_outer_vertices": sum(next_outer_values) / worker_count,
                    "max_processed_private_candidates": max(processed_private_values),
                    "avg_processed_private_candidates": sum(processed_private_values)
                    / worker_count,
                    "max_deferred_private_candidates": max(deferred_private_values),
                    "avg_deferred_private_candidates": sum(deferred_private_values)
                    / worker_count,
                    "max_secure_memory_level": max(secure_memory_values),
                    "avg_secure_memory_level": sum(secure_memory_values)
                    / worker_count,
                    "avg_feedback_overhead_ms": sum(feedback_overhead_values)
                    / worker_count,
                    "sum_feedback_overhead_ms": sum(feedback_overhead_values),
                    "feedback_triggered_workers": sum(feedback_trigger_values),
                    "coscheduling_activated_workers": sum(coscheduling_values),
                }
            )


def write_case_summary(rows, path):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["superstep"], row["phase"])].append(row)

    peak_round_ms = 0.0
    peak_imbalance = 0.0
    peak_active_private = 0
    peak_private_candidates = 0
    peak_secure_memory_level = 0
    trigger_rounds = 0
    migration_count = 0
    deferred_wait_proxy_ms = 0.0
    total_processed_private_candidates = 0
    total_deferred_private_candidates = 0
    feedback_overhead_ms = 0.0
    coscheduling_rounds = 0

    for _, items in grouped.items():
        duration_values = [item["duration_ms"] for item in items]
        avg_duration = sum(duration_values) / len(items)
        if duration_values:
            peak_round_ms = max(peak_round_ms, max(duration_values))
            if avg_duration > 0:
                peak_imbalance = max(peak_imbalance, max(duration_values) / avg_duration)
        peak_active_private = max(
            peak_active_private,
            max(item["active_private_vertices"] for item in items),
        )
        peak_private_candidates = max(
            peak_private_candidates,
            max(item["private_candidates"] for item in items),
        )
        peak_secure_memory_level = max(
            peak_secure_memory_level,
            max(item["secure_memory_level"] for item in items),
        )
        if any(item["feedback_triggered"] > 0 for item in items):
            trigger_rounds += 1
        if any(item["coscheduling_activated"] > 0 for item in items):
            coscheduling_rounds += 1
        migration_count += sum(item["deferred_private_candidates"] for item in items)
        total_processed_private_candidates += sum(
            item["processed_private_candidates"] for item in items
        )
        total_deferred_private_candidates += sum(
            item["deferred_private_candidates"] for item in items
        )
        deferred_wait_proxy_ms += sum(
            item["deferred_private_candidates"] * item["duration_ms"]
            for item in items
        )
        feedback_overhead_ms += sum(item["feedback_overhead_ms"] for item in items)

    avg_wait_time_ms = (
        deferred_wait_proxy_ms / total_processed_private_candidates
        if total_processed_private_candidates > 0
        else 0.0
    )
    avg_feedback_overhead_ms = (
        feedback_overhead_ms / len(grouped) if grouped else 0.0
    )

    with open(path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, delimiter="\t")
        writer.writerow(["metric", "value"])
        writer.writerow(["worker_files", len(set(row["fragment_id"] for row in rows))])
        writer.writerow(["supersteps", len(grouped)])
        writer.writerow(["peak_round_duration_ms", peak_round_ms])
        writer.writerow(["peak_imbalance_max_over_avg", peak_imbalance])
        writer.writerow(["peak_active_private_vertices", peak_active_private])
        writer.writerow(["peak_private_candidates", peak_private_candidates])
        writer.writerow(["peak_secure_memory_level", peak_secure_memory_level])
        writer.writerow(["trigger_rounds", trigger_rounds])
        writer.writerow(["migration_count", migration_count])
        writer.writerow(["avg_wait_time_ms", avg_wait_time_ms])
        writer.writerow(["feedback_overhead_ms", feedback_overhead_ms])
        writer.writerow(["avg_feedback_overhead_ms", avg_feedback_overhead_ms])
        writer.writerow(["coscheduling_rounds", coscheduling_rounds])
        writer.writerow(
            ["total_processed_private_candidates", total_processed_private_candidates]
        )
        writer.writerow(
            ["total_deferred_private_candidates", total_deferred_private_candidates]
        )


def main():
    parser = argparse.ArgumentParser(
        description="Merge and summarize SSSP runtime telemetry files."
    )
    parser.add_argument("case_dir", help="Directory containing sssp_runtime_worker_*.tsv")
    parser.add_argument(
        "--merged-tsv",
        default=None,
        help="Output path for merged raw telemetry TSV. Default: <case_dir>/sssp_runtime_merged.tsv",
    )
    parser.add_argument(
        "--superstep-tsv",
        default=None,
        help="Output path for per-superstep summary TSV. Default: <case_dir>/sssp_runtime_superstep_summary.tsv",
    )
    parser.add_argument(
        "--case-summary-tsv",
        default=None,
        help="Output path for case summary TSV. Default: <case_dir>/sssp_runtime_case_summary.tsv",
    )
    args = parser.parse_args()

    case_dir = os.path.abspath(args.case_dir)
    merged_tsv = args.merged_tsv or os.path.join(case_dir, "sssp_runtime_merged.tsv")
    superstep_tsv = args.superstep_tsv or os.path.join(
        case_dir, "sssp_runtime_superstep_summary.tsv"
    )
    case_summary_tsv = args.case_summary_tsv or os.path.join(
        case_dir, "sssp_runtime_case_summary.tsv"
    )

    rows = read_rows(case_dir)
    write_merged(rows, merged_tsv)
    write_superstep_summary(rows, superstep_tsv)
    write_case_summary(rows, case_summary_tsv)


if __name__ == "__main__":
    main()
