# 2026-04-11 Chapter 3 Runtime Feedback Thought-Equivalent Implementation

## Why This Change Exists

- Chapter 3 in the latest thesis draft no longer needs a literal implementation of
  cross-worker vertex migration and routing-table synchronization inside the current runtime.
- The current CA-side execution path is still built around immutable fragments and the existing
  TA interface, so directly introducing online vertex migration would be very invasive and would
  also require changing `TA`, which is outside the allowed scope.
- The user accepted a CA-only substitute that can still reflect the two main ideas of Chapter 3:
  - large idea: runtime feedback and slow-worker-aware correction
  - small idea: TEE / REE collaborative timing control for private work admission

## Design Mapping

- The large idea is mapped to a lightweight runtime feedback mechanism:
  - each round records per-worker runtime and secure-load proxy
  - workers use `AllGather` to compare local round time with cluster average
  - when slowdown and secure-pressure conditions are met, private work is no longer fully drained
    in one round
  - private candidates are ranked by urgency and only a budgeted subset enters TEE first
- The small idea is mapped to a deferred private queue:
  - low-readiness private candidates are postponed
  - postponed candidates accumulate pending-message count and deferred rounds
  - co-scheduling can admit additional deferred candidates when they become "ready"

## Scope

- Only CA-side code is changed.
- `TA` is not changed.
- Graph partition logic is not changed.
- This is a thought-equivalent runtime implementation for Chapter 3 experiments, not a literal
  online fragment migration system.

## What Changed

- Added a shared runtime feedback helper:
  [runtime_feedback.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/runtime_feedback.h)
  - unified runtime snapshot structure
  - unified per-round stat dumping
  - unified slow-worker detection and control update
  - round-state recording now supports multiple private-processing stages in the same superstep
- Extended runtime flags in:
  [flags.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/flags.h)
  [flags.cc](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/flags.cc)
  - added `coscheduling_min_deferred_rounds`
  - added `coscheduling_min_pending_messages`

## Algorithm Updates

- BFS:
  [bfs.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/bfs/bfs.h)
  [bfs_context.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/bfs/bfs_context.h)
  - fixed the runtime feedback patch so it can participate in global `AllGather`
  - added deferred private queue bookkeeping
  - private candidates can now be ranked, budgeted, deferred, and later re-admitted
  - runtime statistics are dumped per worker

- WCC:
  [wcc.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/wcc/wcc.h)
  [wcc_context.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/wcc/wcc_context.h)
  - introduced TEE-private candidate buffering similar to BFS / SSSP
  - added deferred private queue and pending-count accumulation
  - added urgency-based selection for private label updates
  - added runtime feedback driven budget control and co-scheduling admission
  - added runtime stat dumping per worker

- PageRank:
  [pagerank.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/pagerank/pagerank.h)
  [pagerank_context.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/pagerank/pagerank_context.h)
  - changed private updates from "directly write then secure commit" to "buffer first, commit on admission"
  - this makes deferred admission actually influence the runtime behavior, which is necessary for
    the Chapter 3 idea to be observable
  - added deferred private queue, pending-count accumulation, and round-based readiness
  - added urgency ranking based on rank delta and communication proxy
  - final round forces a full secure flush so output is not left with an unfinished deferred queue
  - added runtime stat dumping per worker

## Metrics Support

- Existing TEE metric dumping is preserved:
  [tee_metrics.h](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/tee_metrics.h)
- Chapter 3 can therefore use:
  - total runtime: from run logs
  - total TEE execution time: from per-worker TEE metric files and later aggregation

## Verification Notes

- Full local linking currently cannot complete on this machine because the build depends on the
  fixed external path `/data/lib/libteec.so`.
- A syntax-level compile check was still completed against:
  [run_app.cc](/Users/liujingkang/Desktop/论文代码文件夹/libgrape-lite-privacy_v2/examples/analytical_apps/run_app.cc)
  - result: passed after this round of code changes
  - note: the syntax check used a temporary local stub for `tee_client_api.h` only to bypass the
    missing OP-TEE SDK header during compilation verification

## Intended Chapter 3 Experiment Modes

- Static baseline:
  - `--runtime_feedback=false`
  - `--tee_ree_coscheduling=false`
- Runtime feedback only:
  - `--runtime_feedback=true`
  - `--tee_ree_coscheduling=false`
- Runtime feedback + collaborative scheduling:
  - `--runtime_feedback=true`
  - `--tee_ree_coscheduling=true`

## Remaining Follow-Up

- A dedicated Chapter 3 batch experiment script still needs to be added so the `4 datasets x 4 algorithms x 3 modes`
  runs can be collected automatically.
- Real cross-worker vertex migration is still intentionally not implemented in this branch.
