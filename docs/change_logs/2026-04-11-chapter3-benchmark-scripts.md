# 2026-04-11 Chapter 3 Benchmark Scripts

## Why This Change Exists

- After the CA-side Chapter 3 runtime mechanism was added, the project still lacked a paper-ready
  batch runner for the actual experiment matrix.
- The target Chapter 3 experiment setting is:
  - `4` datasets
  - `4` algorithms
  - privacy ratio fixed at `70%`
  - `8` workers by default
  - metrics:
    - total runtime
    - total TEE execution time
  - three runtime modes:
    - static execution
    - runtime feedback
    - runtime feedback + collaborative scheduling

## Added Runner

- Added:
  [run_chapter3_runtime_bench.sh](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/run_chapter3_runtime_bench.sh)
- The runner:
  - reuses the existing dataset directory layout under `/home/hust/ljkdataset/...`
  - defaults to:
    - algorithms `sssp,bfs,pagerank,wcc`
    - datasets `web-Google,wiki-topcats,com-lj.ungraph,com-orkut.ungraph`
    - privacy ratio `0.7`
    - workers `8`
  - fixes the partitioning stage to the privacy-aware streaming setting by default:
    - `--segmented_partition=true`
    - `--pesp_partition=true`
  - compares these three Chapter 3 modes by default:
    - `Static`
    - `Feedback`
    - `Feedback+Scheduling`

## Collected Metrics

- For each case, the runner records:
  - `runtime_sec`
  - `total_tee_time_ms`
  - `total_tee_batches`
  - `total_secure_items`
- `total_tee_time_ms` is extracted from the summed worker metric:
  - `sum_total_tee_time_ms`
  - produced via:
    [summarize_tee_workers.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/summarize_tee_workers.py)

## Added Plot Script

- Added:
  [plot_chapter3_runtime_metrics.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/plot_chapter3_runtime_metrics.py)
- The plotting script:
  - normalizes all results against `Static = 1.0` within the same algorithm and dataset
  - produces figures for:
    - `归一化总运行时间`
    - `归一化总TEE执行时间`
  - generates:
    - per-algorithm figures
    - one shared `2 x 2` combined figure for runtime
    - one shared `2 x 2` combined figure for total TEE time

## Validation

- Shell runner validation:
  - `bash -n` passed
  - `--dry-run` expansion was checked for a sample `SSSP + GG + 70% + 8 workers` case
- Python plotting validation:
  - `py_compile` passed
  - the plotting script successfully rendered figures from a synthetic CSV smoke-test

## Usage Snapshot

- Run the full Chapter 3 benchmark:

```bash
./scripts/run_chapter3_runtime_bench.sh \
  --run-app ./build/run_app \
  --workers 8 \
  --ratio 0.7
```

- Plot the results:

```bash
./scripts/plot_chapter3_runtime_metrics.py \
  --csv ./benchmark_outputs/chapter3_runtime/chapter3_runtime_results.csv \
  --figure-dir ./benchmark_outputs/chapter3_runtime/figures
```

## Scope Reminder

- These scripts do not change `TA`.
- These scripts do not change graph partition logic.
- They only automate the Chapter 3 experiment collection and plotting pipeline on top of the
  CA-side thought-equivalent runtime implementation.
