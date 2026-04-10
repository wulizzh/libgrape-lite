# 2026-04-10 Chapter 2 Four-App Benchmark Automation

## Why This Change Exists

- The current Chapter 2 scope is narrowed to four graph algorithms:
  - `sssp`
  - `bfs`
  - `pagerank`
  - `wcc`
- The current experimental matrix is fixed to:
  - `3` datasets: `web-Google (GG)`, `wiki-topcats (TC)`, `com-LiveJournal (LJ)`
  - `70%` privacy ratio
  - `3` partition methods: `Hash`, `Segmented`, privacy-aware streaming partition
  - `8` workers
- The user now needs actual experiment execution plus normalized figures, not only ad-hoc manual runs.

## What Changed

- Added [scripts/run_chapter2_partition_bench.sh](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/run_chapter2_partition_bench.sh) to run the full Chapter 2 benchmark matrix and record:
  - total runtime from the `- run algorithm:` log line
  - maximum single-worker cumulative TEE time via `scripts/summarize_tee_workers.py`
- Added [scripts/plot_chapter2_partition_metrics.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/plot_chapter2_partition_metrics.py) to:
  - normalize runtime and max single-worker TEE time
  - generate grouped bar charts
  - output one figure per algorithm for each metric
- Kept the worker-side TEE summary logic unchanged:
  - the reported TEE metric is still `max(total_tee_time_ms)` across all worker files
- Normalized TSV line endings for the worker TEE summary output and stripped trailing carriage returns when the benchmark runner parses scalar values back into CSV.

## Experimental Conventions Captured In The Scripts

- Formal Chapter 2 runs use `8` MPI workers.
- The privacy ratio is fixed at `0.7` by default.
- Partition method flags are mapped as:
  - `Hash` -> `--segmented_partition=false --pesp_partition=false`
  - `Segmented` -> `--segmented_partition=true --pesp_partition=false`
  - `PESP` -> `--segmented_partition=true --pesp_partition=true`
- The normalization rule for figures is:
  - within the same algorithm and dataset, `Hash = 1.0`
  - `Segmented` and `PESP` are shown relative to that baseline

## Direction Flag Handling

- `sssp`, `bfs`, and `pagerank` follow the dataset-level directed setting:
  - `web-Google` -> directed
  - `wiki-topcats` -> directed
  - `com-LiveJournal` -> undirected
- `wcc` is forced to run without `--directed` so it measures weak connectivity on the undirected view, which matches the intended semantics of WCC.

## Output Layout

- Raw benchmark CSV:
  - `chapter2_partition_results.csv`
- Normalized CSV:
  - `chapter2_partition_normalized.csv`
- Figures:
  - `figures/runtime/<algorithm>_normalized_runtime.*`
  - `figures/max_tee/<algorithm>_normalized_max_tee.*`
