# 2026-04-14 METIS External Partition Support

## Why This Change Exists

- The project needs a new baseline partitioning method based on `METIS`, but the user explicitly does not want to modify the raw datasets under `ljkdataset/`.
- The safest integration path is:
  - keep the existing CA / TA execution model unchanged
  - generate a static partition layout offline
  - let the CA-side graph loader read that external layout during graph loading

## What Changed

- Added a new runtime flag:
  - [flags.cc](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/flags.cc)
  - [flags.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/flags.h)
  - flag name: `--external_partition_file`
- Extended graph-loading spec propagation so the loader can receive that file path:
  - [load_graph_flags.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/load_graph_flags.h)
  - [basic_fragment_loader.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/grape/fragment/basic_fragment_loader.h)
- Updated the EV loader to read explicit `vertex_id partition_id` pairs and override the segmented partition assignment:
  - [ev_fragment_loader.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/grape/fragment/ev_fragment_loader.h)
- Added a standalone preprocessing script:
  - [build_metis_partition.py](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/scripts/build_metis_partition.py)

## External Assignment File Format

- The CA-side loader now accepts a plain-text assignment file with one vertex per line:

```text
vertex_id partition_id
```

- Example:

```text
0 3
1 1
2 3
```

- Comment lines beginning with `#` are ignored.

## Validation Rules Added

- `--external_partition_file` currently requires:
  - `--segmented_partition=true`
  - raw `efile` / `vfile` loading
- It is rejected together with:
  - `--pesp_partition=true`
  - `--deserialize=true`
  - `--rebalance=true`

## METIS Script Assumptions

- The conversion script is tailored to the current `ljkdataset` file layout:
  - vertex lines begin with `vertex_id`
  - edge lines begin with `src dst`
- It does not rewrite the dataset itself.
- It writes new derived files beside the chosen output prefix:
  - `.metis.graph`
  - `.metis.vertex_order.tsv`
  - `.metis.assignments.partK.tsv`

## Usage Snapshot

- Prepare a METIS graph and run `gpmetis` for `8` partitions:

```bash
./scripts/build_metis_partition.py \
  --vfile /home/hust/ljkdataset/web-google-ratio_dataset/web-Google-0.7.v \
  --efile /home/hust/ljkdataset/web-google-ratio_dataset/web-Google.e \
  --parts 8 \
  --output-prefix /tmp/metis/web-Google_ratio07_8w
```

- Use the generated assignment file in `run_app`:

```bash
mpirun -n 8 ./run_app \
  --application sssp \
  --vfile /home/hust/ljkdataset/web-google-ratio_dataset/web-Google-0.7.v \
  --efile /home/hust/ljkdataset/web-google-ratio_dataset/web-Google.e \
  --segmented_partition=true \
  --external_partition_file /tmp/metis/web-Google_ratio07_8w.metis.assignments.part8.tsv
```
