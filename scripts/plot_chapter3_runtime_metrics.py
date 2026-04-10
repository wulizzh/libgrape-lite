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

matplotlib.rcParams["font.family"] = [
    "Songti SC",
    "Times New Roman",
    "DejaVu Serif",
]
matplotlib.rcParams["axes.unicode_minus"] = False
matplotlib.rcParams["legend.fancybox"] = False


MODE_ORDER = ["Static", "Feedback", "Feedback+Scheduling"]
MODE_LABELS = {
    "Static": "静态执行",
    "Feedback": "运行时反馈",
    "Feedback+Scheduling": "反馈+协同调度",
}
MODE_COLORS = {
    "Static": "#2F7FBF",
    "Feedback": "#FF8C1A",
    "Feedback+Scheduling": "#69B34C",
}
MODE_HATCHES = {
    "Static": "///",
    "Feedback": "\\\\\\",
    "Feedback+Scheduling": "xx",
}
ALGORITHM_ORDER = ["sssp", "bfs", "pagerank", "wcc"]
ALGORITHM_CAPTIONS = {
    "sssp": "(a) SSSP",
    "bfs": "(b) BFS",
    "pagerank": "(c) PageRank",
    "wcc": "(d) WCC",
}
DATASET_ORDER = ["GG", "TC", "LJ", "OR"]


def canonical_mode(raw_mode):
    lowered = raw_mode.strip().lower()
    if lowered == "static":
        return "Static"
    if lowered == "feedback":
        return "Feedback"
    if lowered in {
        "feedback+scheduling",
        "feedback_scheduling",
        "feedback-scheduling",
        "feedbackscheduling",
    }:
        return "Feedback+Scheduling"
    return raw_mode.strip()


def load_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            row["mode"] = canonical_mode(row["mode"])
            row["runtime_sec"] = float(row["runtime_sec"])
            row["total_tee_time_ms"] = float(row["total_tee_time_ms"])
            rows.append(row)
    return rows


def normalize_rows(rows):
    grouped = defaultdict(dict)
    for row in rows:
        grouped[(row["algorithm"], row["dataset_abbr"])][row["mode"]] = row

    normalized_rows = []
    for (algorithm, dataset_abbr), mode_rows in sorted(grouped.items()):
        static_row = mode_rows.get("Static")
        if static_row is None:
            raise ValueError(
                f"Missing Static baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )

        runtime_base = static_row["runtime_sec"]
        tee_base = static_row["total_tee_time_ms"]
        if runtime_base <= 0:
            raise ValueError(
                f"Invalid runtime baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )

        tee_values = [mode_rows[mode]["total_tee_time_ms"] for mode in MODE_ORDER]
        zero_tee_group = all(value == 0.0 for value in tee_values)
        if tee_base < 0:
            raise ValueError(
                f"Invalid total tee baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )
        if tee_base == 0 and not zero_tee_group:
            raise ValueError(
                "Static tee baseline is zero but not all tee values are zero for "
                f"algorithm={algorithm}, dataset={dataset_abbr}"
            )

        for mode in MODE_ORDER:
            if mode not in mode_rows:
                raise ValueError(
                    f"Missing mode={mode} for algorithm={algorithm}, dataset={dataset_abbr}"
                )
            row = dict(mode_rows[mode])
            row["normalized_runtime"] = row["runtime_sec"] / runtime_base
            row["normalized_total_tee"] = (
                1.0 if zero_tee_group else row["total_tee_time_ms"] / tee_base
            )
            normalized_rows.append(row)
    return normalized_rows


def write_normalized_csv(rows, output_path):
    fieldnames = [
        "algorithm",
        "dataset",
        "dataset_abbr",
        "privacy_ratio",
        "workers",
        "mode",
        "directed_flag",
        "runtime_sec",
        "total_tee_time_ms",
        "total_tee_batches",
        "total_secure_items",
        "normalized_runtime",
        "normalized_total_tee",
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


def collect_metric_series(rows, algorithm, metric_key):
    x = np.arange(len(DATASET_ORDER), dtype=float)
    width = 0.22
    offsets = [-width, 0.0, width]

    metric_values = []
    series = []
    for idx, mode in enumerate(MODE_ORDER):
        y = []
        for dataset_abbr in DATASET_ORDER:
            matching = [
                row
                for row in rows
                if row["algorithm"] == algorithm
                and row["dataset_abbr"] == dataset_abbr
                and row["mode"] == mode
            ]
            if len(matching) != 1:
                raise ValueError(
                    f"Expected one row for algorithm={algorithm}, dataset={dataset_abbr}, mode={mode}, got {len(matching)}"
                )
            value = matching[0][metric_key]
            y.append(value)
            metric_values.append(value)
        series.append((mode, x + offsets[idx], y, width))
    return series, metric_values


def draw_series(ax, series):
    handles = []
    labels = []
    for mode, positions, y, width in series:
        bars = ax.bar(
            positions,
            y,
            width=width,
            label=MODE_LABELS[mode],
            color=MODE_COLORS[mode],
            edgecolor="#222222",
            linewidth=1.0,
            hatch=MODE_HATCHES[mode],
            zorder=3,
        )
        handles.append(bars[0])
        labels.append(MODE_LABELS[mode])
    return handles, labels


def style_axis(ax, ylabel, ymax, show_xlabel=True, show_ylabel=True):
    upper = max(1.12, ymax * 1.15)
    ax.set_ylim(0.0, upper)
    ax.axhline(1.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.set_xticks(np.arange(len(DATASET_ORDER), dtype=float))
    ax.set_xticklabels(DATASET_ORDER)
    ax.set_xlabel("数据集" if show_xlabel else "", fontsize=12, labelpad=2)
    ax.set_ylabel(ylabel if show_ylabel else "", fontsize=12, labelpad=4)
    ax.tick_params(
        axis="both",
        which="major",
        labelsize=11,
        direction="out",
        length=4,
        width=0.9,
    )
    ax.set_axisbelow(True)
    ax.margins(x=0.06)
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(1.0)
        spine.set_color("#4C4C4C")


def render_algorithm_plot(rows, algorithm, metric_key, ylabel, output_dir):
    fig, ax = plt.subplots(figsize=(9.4, 4.9))
    series, metric_values = collect_metric_series(rows, algorithm, metric_key)
    handles, labels = draw_series(ax, series)
    finite_metric_values = finite_values(metric_values)
    ymax = max(finite_metric_values) if finite_metric_values else 1.0
    style_axis(ax, ylabel, ymax)
    ax.legend(
        handles,
        labels,
        loc="upper center",
        ncol=3,
        frameon=True,
        edgecolor="#7A7A7A",
        framealpha=1.0,
        fontsize=11,
    )
    fig.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    base_name = f"{algorithm}_{metric_key}"
    png_path = os.path.join(output_dir, f"{base_name}.png")
    pdf_path = os.path.join(output_dir, f"{base_name}.pdf")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def render_combined_plot(rows, metric_key, ylabel, output_dir, base_name):
    fig, axes = plt.subplots(2, 2, figsize=(10.6, 8.9))
    axes = axes.flatten()

    all_metric_values = []
    for algorithm in ALGORITHM_ORDER:
        _, metric_values = collect_metric_series(rows, algorithm, metric_key)
        all_metric_values.extend(metric_values)
    finite_metric_values = finite_values(all_metric_values)
    ymax = max(finite_metric_values) if finite_metric_values else 1.0

    legend_handles = None
    legend_labels = None
    for idx, algorithm in enumerate(ALGORITHM_ORDER):
        ax = axes[idx]
        series, _ = collect_metric_series(rows, algorithm, metric_key)
        handles, labels = draw_series(ax, series)
        if legend_handles is None:
            legend_handles = handles
            legend_labels = labels

        style_axis(ax, ylabel, ymax)
        ax.text(
            0.5,
            -0.16,
            ALGORITHM_CAPTIONS[algorithm],
            transform=ax.transAxes,
            ha="center",
            va="top",
            fontsize=13,
        )

    fig.legend(
        legend_handles,
        legend_labels,
        loc="upper center",
        ncol=3,
        frameon=True,
        edgecolor="#7A7A7A",
        framealpha=1.0,
        bbox_to_anchor=(0.5, 0.985),
        fontsize=12,
        handlelength=2.0,
        columnspacing=1.8,
    )
    fig.subplots_adjust(
        left=0.08,
        right=0.985,
        top=0.93,
        bottom=0.12,
        wspace=0.24,
        hspace=0.34,
    )

    os.makedirs(output_dir, exist_ok=True)
    png_path = os.path.join(output_dir, f"{base_name}.png")
    pdf_path = os.path.join(output_dir, f"{base_name}.pdf")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Normalize Chapter 3 runtime metrics against Static=1.0 "
            "for each algorithm and dataset, then generate grouped bar charts."
        )
    )
    parser.add_argument("--csv", required=True, help="Raw result CSV from the benchmark runner.")
    parser.add_argument(
        "--normalized-csv",
        default="",
        help="Optional output path for normalized CSV. Defaults to <figure-dir>/chapter3_runtime_normalized.csv",
    )
    parser.add_argument(
        "--figure-dir",
        default="./benchmark_outputs/chapter3_runtime/figures",
        help="Output directory for figures.",
    )
    args = parser.parse_args()

    rows = load_rows(args.csv)
    normalized_rows = normalize_rows(rows)

    normalized_csv = (
        args.normalized_csv
        if args.normalized_csv
        else os.path.join(args.figure_dir, "chapter3_runtime_normalized.csv")
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
                "归一化总运行时间",
                os.path.join(args.figure_dir, "runtime"),
            )
        )
        generated.extend(
            render_algorithm_plot(
                algorithm_rows,
                algorithm,
                "normalized_total_tee",
                "归一化总TEE执行时间",
                os.path.join(args.figure_dir, "total_tee"),
            )
        )

    generated.extend(
        render_combined_plot(
            normalized_rows,
            "normalized_runtime",
            "归一化总运行时间",
            os.path.join(args.figure_dir, "runtime"),
            "combined_normalized_runtime_2x2",
        )
    )
    generated.extend(
        render_combined_plot(
            normalized_rows,
            "normalized_total_tee",
            "归一化总TEE执行时间",
            os.path.join(args.figure_dir, "total_tee"),
            "combined_normalized_total_tee_2x2",
        )
    )

    print(f"normalized_csv={normalized_csv}")
    for path in generated:
        print(f"figure={path}")


if __name__ == "__main__":
    main()
