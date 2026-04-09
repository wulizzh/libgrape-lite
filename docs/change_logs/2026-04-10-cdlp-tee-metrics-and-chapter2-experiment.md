# 2026-04-10 CDLP TEE Metrics And Chapter 2 Experiment Notes

## Why This Change Exists

- `cdlp_selective` was able to run before the recent CA-side instrumentation changes.
- The most suspicious regression source is not the core label-propagation logic itself, but the new tee timing path added on every `is_equal` call in the hottest comparison loop.
- Chapter 2 experiments now need to run with 8 workers, and the tee metric must be reported as the maximum cumulative tee time among all workers.

## What Changed

- Kept the previous CDLP safety fixes:
  - the valid-label bitmap now covers `frag.Vertices()` instead of only `InnerVertices()`
  - `frag.GetVertex()` is checked before probing the valid-label bitmap
- Reduced CDLP tee timing overhead:
  - per-call timing is still used for the low-frequency equality checks in `PEval()` and the final old/new label comparison
  - the hot sorted-label comparison loop in `update_label_fast_selected()` now measures one aggregated block instead of timing every single equality call
- Made CDLP tee metric accumulation safe under parallel execution with a context-level mutex.
- Fixed a GCC 7 template deduction issue in the new CDLP tee metric helper so the server build can pass.
- Added `scripts/summarize_tee_workers.py` to summarize `*_tee_worker_*.tsv` files and extract:
  - `max_total_tee_time_ms`
  - `max_worker_fragment_id`
  - worker count

## Chapter 2 Execution Rules

- Formal experiments use 8 workers.
- The tee indicator is:
  - maximum single-worker cumulative tee execution time
- Operationally, this means:
  - every worker writes one tee stats file
  - the reported metric is the maximum `total_tee_time_ms` across all worker files

## Chapter 2 Experiment Matrix

- Algorithms:
  - `sssp`
  - `bfs`
  - `pagerank`
  - `cdlp`
  - `wcc`
- Datasets at `70%` privacy ratio:
  - `web-Google (GG)`
  - `wiki-topcats (TC)`
  - `com-LiveJournal (LJ)`
  - `com-Orkut (OR)`
- Partition methods:
  - `Hash`
  - `Segmented`
  - privacy-aware streaming partition (`PESP` in the code path)
- Metrics:
  - total runtime
  - maximum single-worker cumulative tee time

## Important Scope Reminder

- Only change `CA`.
- Do not modify `TA`.
- Do not modify datasets.
- Do not modify the graph partition layer for algorithm migration work.
- `bfs / wcc / pagerank` currently use CA-side tee-assisted paths added without changing TA.
