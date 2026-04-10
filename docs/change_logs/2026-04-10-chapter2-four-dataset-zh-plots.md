# 2026-04-10 Chapter 2 Four-Dataset And Chinese Plot Update

## Why This Change Exists

- Chapter 2 experiments now need to cover `4` datasets instead of `3`.
- `com-Orkut (OR)` has been prepared on the server and should be included in the same benchmark workflow.
- The figure style also needs to match the paper draft more closely:
  - Chinese axis labels
  - no top title text
  - the legend item for privacy-aware streaming partition should use Chinese wording

## What Changed

- Extended [scripts/run_chapter2_partition_bench.sh](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/run_chapter2_partition_bench.sh) so the default Chapter 2 dataset set becomes:
  - `web-Google (GG)`
  - `wiki-topcats (TC)`
  - `com-LiveJournal (LJ)`
  - `com-Orkut (OR)`
- Added the `com-Orkut` dataset specification with:
  - dataset name `com-orkut.ungraph`
  - abbreviation `OR`
  - directory `/home/hust/ljkdataset/com-orkut.ungraph_dateset`
  - edge file `com-orkut.ungraph.e`
  - vertex prefix `com-orkut.ungraph`
  - undirected mode
- Updated [scripts/plot_chapter2_partition_metrics.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/plot_chapter2_partition_metrics.py) to:
  - support `GG / TC / LJ / OR`
  - use `数据集` as the x-axis label
  - use Chinese y-axis labels:
    - `归一化运行时间`
    - `归一化最大单个节点累计TEE执行时间`
  - remove the top plot title
  - rename the legend item `Privacy-aware Streaming` to `隐私感知流式划分`
  - prefer Chinese-capable fonts when rendering locally

## Scope Reminder

- This update changes experiment automation and plot presentation only.
- It does not modify `TA`.
- It does not change the benchmark normalization rule:
  - within the same algorithm and dataset, `Hash = 1.0`
