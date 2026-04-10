#!/usr/bin/env python3

import argparse
import csv
import math
import os
from collections import defaultdict

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-codex")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


METHOD_ORDER = ["Hash", "Segmented", "PESP"]
METHOD_LABELS = {
    "Hash": "Hash",
    "Segmented": "Segmented",
    "PESP": "Privacy-aware Streaming",
}
METHOD_COLORS = {
    "Hash": "#4C78A8",
    "Segmented": "#F58518",
    "PESP": "#54A24B",
}
ALGORITHM_ORDER = ["sssp", "bfs", "pagerank", "wcc"]
ALGORITHM_LABELS = {
    "sssp": "SSSP",
    "bfs": "BFS",
    "pagerank": "PageRank",
    "wcc": "WCC",
}
DATASET_ORDER = ["GG", "TC", "LJ"]


def load_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            row["runtime_sec"] = float(row["runtime_sec"])
            row["max_tee_time_ms"] = float(row["max_tee_time_ms"])
            rows.append(row)
    return rows


def normalize_rows(rows):
    grouped = defaultdict(dict)
    for row in rows:
        grouped[(row["algorithm"], row["dataset_abbr"])][row["method"]] = row

    normalized_rows = []
    for (algorithm, dataset_abbr), method_rows in sorted(grouped.items()):
        hash_row = method_rows.get("Hash")
        if hash_row is None:
            raise ValueError(
                f"Missing Hash baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )
        runtime_base = hash_row["runtime_sec"]
        tee_base = hash_row["max_tee_time_ms"]
        if runtime_base <= 0:
            raise ValueError(
                f"Invalid runtime baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )
        if tee_base <= 0:
            raise ValueError(
                f"Invalid tee baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )

        for method in METHOD_ORDER:
            if method not in method_rows:
                raise ValueError(
                    f"Missing method={method} for algorithm={algorithm}, dataset={dataset_abbr}"
                )
            row = dict(method_rows[method])
            row["normalized_runtime"] = row["runtime_sec"] / runtime_base
            row["normalized_max_tee"] = row["max_tee_time_ms"] / tee_base
            normalized_rows.append(row)
    return normalized_rows


def write_normalized_csv(rows, output_path):
    fieldnames = [
        "algorithm",
        "dataset",
        "dataset_abbr",
        "privacy_ratio",
        "workers",
        "method",
        "directed_flag",
        "runtime_sec",
        "max_tee_time_ms",
        "normalized_runtime",
        "normalized_max_tee",
        "tee_summary_path",
        "case_dir",
        "log_path",
    ]
    with open(output_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def finite_values(values):
    return [value for value in values if math.isfinite(value)]


def render_algorithm_plot(rows, algorithm, metric_key, ylabel, title_suffix, output_dir):
    fig, ax = plt.subplots(figsize=(8.6, 4.8))
    x = np.arange(len(DATASET_ORDER), dtype=float)
    width = 0.22
    offsets = [-width, 0.0, width]

    metric_values = []
    for idx, method in enumerate(METHOD_ORDER):
        y = []
        for dataset_abbr in DATASET_ORDER:
            matching = [
                row
                for row in rows
                if row["algorithm"] == algorithm
                and row["dataset_abbr"] == dataset_abbr
                and row["method"] == method
            ]
            if len(matching) != 1:
                raise ValueError(
                    f"Expected one row for algorithm={algorithm}, dataset={dataset_abbr}, method={method}, got {len(matching)}"
                )
            value = matching[0][metric_key]
            y.append(value)
            metric_values.append(value)
        ax.bar(
            x + offsets[idx],
            y,
            width=width,
            label=METHOD_LABELS[method],
            color=METHOD_COLORS[method],
            edgecolor="black",
            linewidth=0.7,
        )

    finite_metric_values = finite_values(metric_values)
    ymax = max(finite_metric_values) if finite_metric_values else 1.0
    ax.set_ylim(0.0, max(1.15, ymax * 1.15))
    ax.axhline(1.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.set_xticks(x)
    ax.set_xticklabels(DATASET_ORDER)
    ax.set_xlabel("Dataset")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{ALGORITHM_LABELS.get(algorithm, algorithm.upper())} {title_suffix}")
    ax.grid(axis="y", linestyle=":", linewidth=0.8, alpha=0.55)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    fig.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    base_name = f"{algorithm}_{metric_key}"
    png_path = os.path.join(output_dir, f"{base_name}.png")
    pdf_path = os.path.join(output_dir, f"{base_name}.pdf")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Normalize Chapter 2 partition metrics against Hash=1.0 "
            "for each algorithm and dataset, then generate grouped bar charts."
        )
    )
    parser.add_argument("--csv", required=True, help="Raw result CSV from the benchmark runner.")
    parser.add_argument(
        "--normalized-csv",
        default="",
        help="Optional output path for normalized CSV. Defaults to <figure-dir>/chapter2_partition_normalized.csv",
    )
    parser.add_argument(
        "--figure-dir",
        default="./benchmark_outputs/chapter2_partition/figures",
        help="Output directory for figures.",
    )
    args = parser.parse_args()

    rows = load_rows(args.csv)
    normalized_rows = normalize_rows(rows)

    normalized_csv = (
        args.normalized_csv
        if args.normalized_csv
        else os.path.join(args.figure_dir, "chapter2_partition_normalized.csv")
    )
    os.makedirs(args.figure_dir, exist_ok=True)
    write_normalized_csv(normalized_rows, normalized_csv)

    generated = []
    for algorithm in ALGORITHM_ORDER:
        algorithm_rows = [row for row in normalized_rows if row["algorithm"] == algorithm]
        if not algorithm_rows:
            continue
        generated.extend(
            render_algorithm_plot(
                algorithm_rows,
                algorithm,
                "normalized_runtime",
                "Normalized Runtime (Hash = 1.0)",
                "Normalized Total Runtime",
                os.path.join(args.figure_dir, "runtime"),
            )
        )
        generated.extend(
            render_algorithm_plot(
                algorithm_rows,
                algorithm,
                "normalized_max_tee",
                "Normalized Max Single-Worker TEE Time (Hash = 1.0)",
                "Normalized Max Single-Worker TEE Time",
                os.path.join(args.figure_dir, "max_tee"),
            )
        )

    print(f"normalized_csv={normalized_csv}")
    for path in generated:
        print(f"figure={path}")


if __name__ == "__main__":
    main()
