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


METHOD_PRIORITY = ["Hash", "Segmented", "PESP", "METIS"]
METHOD_LABELS = {
    "Hash": "Hash",
    "Segmented": "Segmented",
    "PESP": "隐私感知流式划分",
    "METIS": "METIS",
}
METHOD_COLORS = {
    "Hash": "#2F7FBF",
    "Segmented": "#FF8C1A",
    "PESP": "#69B34C",
    "METIS": "#8E6BBE",
}
METHOD_HATCHES = {
    "Hash": "///",
    "Segmented": "\\\\\\",
    "PESP": "xx",
    "METIS": "..",
}
ALGORITHM_ORDER = ["sssp", "bfs", "pagerank", "wcc"]
ALGORITHM_LABELS = {
    "sssp": "SSSP",
    "bfs": "BFS",
    "pagerank": "PageRank",
    "wcc": "CC",
}
ALGORITHM_CAPTIONS = {
    "sssp": "(a) SSSP",
    "bfs": "(b) BFS",
    "pagerank": "(c) PageRank",
    "wcc": "(d) CC",
}
DATASET_ORDER = ["GG", "TC", "LJ", "OR"]


def load_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            row.setdefault("external_partition_file", "")
            row["runtime_sec"] = float(row["runtime_sec"])
            row["max_tee_time_ms"] = float(row["max_tee_time_ms"])
            rows.append(row)
    return rows


def normalize_rows(rows):
    method_order = infer_method_order(rows)
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

        tee_values = [method_rows[method]["max_tee_time_ms"] for method in method_order]
        zero_tee_group = all(value == 0.0 for value in tee_values)
        if tee_base < 0:
            raise ValueError(
                f"Invalid tee baseline for algorithm={algorithm}, dataset={dataset_abbr}"
            )
        if tee_base == 0 and not zero_tee_group:
            raise ValueError(
                "Hash tee baseline is zero but not all tee values are zero for "
                f"algorithm={algorithm}, dataset={dataset_abbr}"
            )

        for method in method_order:
            if method not in method_rows:
                raise ValueError(
                    f"Missing method={method} for algorithm={algorithm}, dataset={dataset_abbr}"
                )
            row = dict(method_rows[method])
            row["normalized_runtime"] = row["runtime_sec"] / runtime_base
            # If all three tee measurements are zero, keep the normalized baseline flat at 1.0
            # so the chart can still render the completed experiment without inventing a ratio.
            row["normalized_max_tee"] = (
                1.0 if zero_tee_group else row["max_tee_time_ms"] / tee_base
            )
            normalized_rows.append(row)
    return normalized_rows, method_order


def infer_method_order(rows):
    methods_in_rows = {row["method"] for row in rows}
    if "Hash" not in methods_in_rows:
        raise ValueError("Missing Hash baseline in the raw CSV.")

    ordered = [method for method in METHOD_PRIORITY if method in methods_in_rows]
    extras = sorted(methods_in_rows - set(METHOD_PRIORITY))
    return ordered + extras


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
        "external_partition_file",
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


def load_schematic_metric_rows(csv_path, metric_key):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            rows.append(
                {
                    "algorithm": row["algorithm"].strip(),
                    "dataset_abbr": row["dataset_abbr"].strip(),
                    "method": row["method"].strip(),
                    metric_key: float(row[metric_key]),
                }
            )
    return rows


def collect_metric_series(rows, algorithm, metric_key):
    method_order = infer_method_order(rows)
    x = np.arange(len(DATASET_ORDER), dtype=float)
    cluster_width = 0.78
    width = cluster_width / max(len(method_order), 1)
    offsets = np.linspace(
        -cluster_width / 2 + width / 2,
        cluster_width / 2 - width / 2,
        num=len(method_order),
    )

    metric_values = []
    series = []
    for idx, method in enumerate(method_order):
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
        series.append((method, x + offsets[idx], y, width))
    return series, metric_values, method_order


def draw_series(ax, series):
    handles = []
    labels = []
    for method, positions, y, width in series:
        label = METHOD_LABELS.get(method, method)
        color = METHOD_COLORS.get(method, "#7A7A7A")
        hatch = METHOD_HATCHES.get(method, "")
        bars = ax.bar(
            positions,
            y,
            width=width,
            label=label,
            color=color,
            edgecolor="#222222",
            linewidth=1.0,
            hatch=hatch,
            zorder=3,
        )
        handles.append(bars[0])
        labels.append(label)
    return handles, labels


def style_axis(
    ax,
    ylabel,
    ymax,
    show_xlabel=True,
    show_ylabel=True,
    ymin=0.0,
    ymax_override=None,
    yticks=None,
):
    upper = ymax_override if ymax_override is not None else max(1.15, ymax * 1.15)
    ax.set_ylim(ymin, upper)
    ax.axhline(1.0, color="#666666", linestyle="--", linewidth=1.0)
    if yticks is not None:
        ax.set_yticks(yticks)
    ax.set_xticks(np.arange(len(DATASET_ORDER), dtype=float))
    ax.set_xticklabels(DATASET_ORDER)
    ax.set_xlabel("数据集" if show_xlabel else "", fontsize=12, labelpad=2)
    ax.set_ylabel(ylabel if show_ylabel else "", fontsize=12, labelpad=4)
    ax.tick_params(axis="both", which="major", labelsize=11, direction="out", length=4, width=0.9)
    ax.set_axisbelow(True)
    ax.margins(x=0.06)
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(1.0)
        spine.set_color("#4C4C4C")


def render_algorithm_plot(rows, algorithm, metric_key, ylabel, output_dir):
    fig, ax = plt.subplots(figsize=(9.4, 4.9))
    series, metric_values, method_order = collect_metric_series(rows, algorithm, metric_key)
    handles, labels = draw_series(ax, series)
    finite_metric_values = finite_values(metric_values)
    ymax = max(finite_metric_values) if finite_metric_values else 1.0
    style_axis(ax, ylabel, ymax)
    ax.legend(
        handles,
        labels,
        loc="upper center",
        ncol=min(len(method_order), 4),
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


def render_combined_plot(
    rows,
    metric_key,
    ylabel,
    output_dir,
    base_name=None,
    ymin=0.0,
    ymax_override=None,
    yticks=None,
):
    fig, axes = plt.subplots(2, 2, figsize=(10.6, 8.9))
    axes = axes.flatten()

    all_metric_values = []
    method_order = infer_method_order(rows)
    for algorithm in ALGORITHM_ORDER:
        algorithm_rows = [row for row in rows if row["algorithm"] == algorithm]
        if not algorithm_rows:
            continue
        _, metric_values, _ = collect_metric_series(algorithm_rows, algorithm, metric_key)
        all_metric_values.extend(metric_values)
    finite_metric_values = finite_values(all_metric_values)
    ymax = max(finite_metric_values) if finite_metric_values else 1.0

    legend_handles = None
    legend_labels = None
    for idx, algorithm in enumerate(ALGORITHM_ORDER):
        ax = axes[idx]
        algorithm_rows = [row for row in rows if row["algorithm"] == algorithm]
        if not algorithm_rows:
            ax.axis("off")
            continue
        series, _, _ = collect_metric_series(algorithm_rows, algorithm, metric_key)
        handles, labels = draw_series(ax, series)
        if legend_handles is None:
            legend_handles = handles
            legend_labels = labels

        style_axis(
            ax,
            ylabel,
            ymax,
            show_xlabel=True,
            show_ylabel=True,
            ymin=ymin,
            ymax_override=ymax_override,
            yticks=yticks,
        )
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
        ncol=min(len(method_order), 4),
        frameon=True,
        edgecolor="#7A7A7A",
        framealpha=1.0,
        bbox_to_anchor=(0.5, 0.985),
        fontsize=12,
        handlelength=2.0,
        columnspacing=1.8,
    )
    fig.subplots_adjust(left=0.08, right=0.985, top=0.93, bottom=0.12, wspace=0.24, hspace=0.34)

    os.makedirs(output_dir, exist_ok=True)
    base_name = base_name if base_name else f"combined_{metric_key}_2x2"
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
    parser.add_argument(
        "--schematic-runtime-csv",
        default="",
        help="Optional CSV for a hand-crafted normalized runtime schematic figure.",
    )
    parser.add_argument(
        "--schematic-max-tee-csv",
        default="",
        help="Optional CSV for a hand-crafted normalized max-TEE schematic figure.",
    )
    args = parser.parse_args()

    rows = load_rows(args.csv)
    normalized_rows, _ = normalize_rows(rows)

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
                "归一化运行时间",
                os.path.join(args.figure_dir, "runtime"),
            )
        )
        generated.extend(
            render_algorithm_plot(
                algorithm_rows,
                algorithm,
                "normalized_max_tee",
                "归一化最大单个worker累计TEE执行时间",
                os.path.join(args.figure_dir, "max_tee"),
            )
        )

    generated.extend(
        render_combined_plot(
            normalized_rows,
            "normalized_runtime",
            "归一化运行时间",
            os.path.join(args.figure_dir, "runtime"),
        )
    )
    generated.extend(
        render_combined_plot(
            normalized_rows,
            "normalized_max_tee",
            "归一化最大单个worker累计TEE执行时间",
            os.path.join(args.figure_dir, "max_tee"),
        )
    )

    if args.schematic_runtime_csv:
        schematic_rows = load_schematic_metric_rows(
            args.schematic_runtime_csv, "normalized_runtime"
        )
        generated.extend(
            render_combined_plot(
                schematic_rows,
                "normalized_runtime",
                "归一化运行时间",
                os.path.join(args.figure_dir, "runtime"),
                base_name="示意图",
                ymin=0.0,
                ymax_override=1.2,
                yticks=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2],
            )
        )

    if args.schematic_max_tee_csv:
        schematic_rows = load_schematic_metric_rows(
            args.schematic_max_tee_csv, "normalized_max_tee"
        )
        generated.extend(
            render_combined_plot(
                schematic_rows,
                "normalized_max_tee",
                "归一化最大单个worker累计TEE执行时间",
                os.path.join(args.figure_dir, "max_tee"),
                base_name="示意图",
                ymin=0.0,
                ymax_override=1.2,
                yticks=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2],
            )
        )

    print(f"normalized_csv={normalized_csv}")
    for path in generated:
        print(f"figure={path}")


if __name__ == "__main__":
    main()
