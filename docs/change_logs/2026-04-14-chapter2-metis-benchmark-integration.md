# 2026-04-14 Chapter 2 METIS Benchmark Integration

## Why This Change Exists

- The project can now load an external partition assignment file, so the next step is to make the Chapter 2 benchmark pipeline use that capability directly.
- The user should not need to manually:
  - convert `ljkdataset` files into METIS input
  - run `gpmetis`
  - hand-copy the resulting assignment path into each experiment command

## What Changed

- Updated the Chapter 2 benchmark runner:
  - [run_chapter2_partition_bench.sh](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/run_chapter2_partition_bench.sh)
- The runner now supports a fourth method:
  - `METIS`
- Added new runner options:
  - `--metis-script`
  - `--gpmetis-bin`
  - `--metis-output-dir`
  - `--metis-sort-tmpdir`
  - `--force-rebuild-metis`

## METIS Workflow In The Runner

- When a case uses `method=METIS`, the runner now:
  1. derives a cache directory under `metis/<dataset>/ratio_<ratio>/workers_<n>/`
  2. invokes
     [build_metis_partition.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/build_metis_partition.py)
     if the cached assignment file is missing
  3. reuses the generated assignment file across all algorithms for the same dataset, privacy ratio, and worker count
  4. passes that file to `run_app` via `--external_partition_file`

## CSV Change

- The Chapter 2 raw results CSV now includes one extra column:
  - `external_partition_file`
- For `Hash`, `Segmented`, and `PESP`, this column is empty.
- For `METIS`, it records the actual assignment file path used by the run.

## Plotting Update

- Updated:
  - [plot_chapter2_partition_metrics.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/plot_chapter2_partition_metrics.py)
- The plotting script no longer assumes exactly three methods.
- It now:
  - infers the method list from the CSV
  - keeps `Hash` as the normalization baseline
  - supports optional `METIS` bars without breaking the existing three-method plots

## Usage Snapshot

- Run the original Chapter 2 setup:

```bash
./scripts/run_chapter2_partition_bench.sh \
  --run-app ./build/run_app \
  --methods Hash,Segmented,PESP
```

- Run with `METIS` added:

```bash
./scripts/run_chapter2_partition_bench.sh \
  --run-app ./build/run_app \
  --methods Hash,Segmented,PESP,METIS \
  --workers 8 \
  --ratio 0.7
```
