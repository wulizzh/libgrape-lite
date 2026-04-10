#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_chapter3_runtime_bench.sh [options]

Options:
  --run-app PATH          Path to the run_app binary. Default: ./run_app
  --workers N             Number of MPI workers. Default: 8
  --ratio VALUE           Privacy ratio suffix in vertex files. Default: 0.7
  --sssp-source ID        Source vertex for SSSP. Default: 0
  --bfs-source ID         Source vertex for BFS. Default: 0
  --pr-d VALUE            PageRank damping factor. Default: 0.85
  --pr-mr N               PageRank max rounds. Default: 10
  --app-concurrency N     Optional --app_concurrency forwarded to run_app
  --output-dir PATH       Base directory for logs and reports.
                          Default: ./benchmark_outputs/chapter3_runtime
  --csv PATH              Output CSV path.
                          Default: <output-dir>/chapter3_runtime_results.csv
  --algorithms LIST       Comma-separated app names.
                          Default: sssp,bfs,pagerank,wcc
  --datasets LIST         Comma-separated dataset names or abbreviations.
                          Default: web-Google,wiki-topcats,com-lj.ungraph,com-orkut.ungraph
  --mode NAME=ARGS        Repeatable. Defines one runtime mode and its flags.
                          If omitted, defaults to:
                          Static=--segmented_partition=true --pesp_partition=true --runtime_feedback=false --tee_ree_coscheduling=false
                          Feedback=--segmented_partition=true --pesp_partition=true --runtime_feedback=true --tee_ree_coscheduling=false
                          Feedback+Scheduling=--segmented_partition=true --pesp_partition=true --runtime_feedback=true --tee_ree_coscheduling=true
  --dry-run               Print commands without executing them.
  --help                  Show this help message.

Examples:
  ./run_chapter3_runtime_bench.sh \
    --run-app ./run_app \
    --workers 8

  ./run_chapter3_runtime_bench.sh \
    --run-app /home/hust/codex_runs/libgrape-lite-privacy_v3/build/run_app \
    --datasets GG,TC \
    --algorithms sssp,bfs \
    --mode 'Feedback=--segmented_partition=true --pesp_partition=true --runtime_feedback=true'
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_APP="${RUN_APP:-./run_app}"
WORKERS="${WORKERS:-8}"
PRIVACY_RATIO="${PRIVACY_RATIO:-0.7}"
SSSP_SOURCE="${SSSP_SOURCE:-0}"
BFS_SOURCE="${BFS_SOURCE:-0}"
PR_D="${PR_D:-0.85}"
PR_MR="${PR_MR:-10}"
APP_CONCURRENCY="${APP_CONCURRENCY:-}"
OUTPUT_DIR="${OUTPUT_DIR:-./benchmark_outputs/chapter3_runtime}"
CSV_OUTPUT="${CSV_OUTPUT:-}"
DRY_RUN=false

DEFAULT_ALGORITHMS=("sssp" "bfs" "pagerank" "wcc")
DEFAULT_DATASETS=("web-Google" "wiki-topcats" "com-lj.ungraph" "com-orkut.ungraph")

SELECTED_ALGORITHMS=()
SELECTED_DATASETS=()
MODE_SPECS=()

DATASET_SPECS=(
  "web-Google|GG|/home/hust/ljkdataset/web-google-ratio_dataset|web-Google.e|web-Google|true"
  "wiki-topcats|TC|/home/hust/ljkdataset/wiki-topcats-ratio_dataset|wiki-topcats.e|wiki-topcats|true"
  "com-lj.ungraph|LJ|/home/hust/ljkdataset/com-lj.ungraph-ratio_dataset|com-lj.ungraph.e|com-lj.ungraph|false"
  "com-orkut.ungraph|OR|/home/hust/ljkdataset/com-orkut.ungraph_dateset|com-orkut.ungraph.e|com-orkut.ungraph|false"
)

contains_exact() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

canonical_algorithm() {
  local raw="$1"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    sssp)
      printf 'sssp\n'
      ;;
    bfs)
      printf 'bfs\n'
      ;;
    pagerank|pr)
      printf 'pagerank\n'
      ;;
    wcc|cc)
      printf 'wcc\n'
      ;;
    *)
      return 1
      ;;
  esac
}

canonical_dataset() {
  local raw="$1"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    web-google|gg)
      printf 'web-Google\n'
      ;;
    wiki-topcats|tc)
      printf 'wiki-topcats\n'
      ;;
    com-lj.ungraph|lj|livejournal|com-livejournal)
      printf 'com-lj.ungraph\n'
      ;;
    com-orkut.ungraph|orkut|or)
      printf 'com-orkut.ungraph\n'
      ;;
    *)
      return 1
      ;;
  esac
}

ratio_to_percent() {
  local ratio="$1"
  awk -v ratio="$ratio" 'BEGIN { printf "%d", ratio * 100 }'
}

sanitize_name() {
  local raw="$1"
  raw="${raw//[^A-Za-z0-9._+-]/_}"
  printf '%s\n' "$raw"
}

effective_directed_flag() {
  local algorithm="$1"
  local dataset_directed="$2"
  if [[ "$algorithm" == "wcc" ]]; then
    printf 'false\n'
  else
    printf '%s\n' "$dataset_directed"
  fi
}

default_mode_specs() {
  MODE_SPECS+=(
    "Static=--segmented_partition=true --pesp_partition=true --runtime_feedback=false --tee_ree_coscheduling=false"
    "Feedback=--segmented_partition=true --pesp_partition=true --runtime_feedback=true --tee_ree_coscheduling=false"
    "Feedback+Scheduling=--segmented_partition=true --pesp_partition=true --runtime_feedback=true --tee_ree_coscheduling=true"
  )
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-app)
      RUN_APP="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --ratio)
      PRIVACY_RATIO="$2"
      shift 2
      ;;
    --sssp-source)
      SSSP_SOURCE="$2"
      shift 2
      ;;
    --bfs-source)
      BFS_SOURCE="$2"
      shift 2
      ;;
    --pr-d)
      PR_D="$2"
      shift 2
      ;;
    --pr-mr)
      PR_MR="$2"
      shift 2
      ;;
    --app-concurrency)
      APP_CONCURRENCY="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --csv)
      CSV_OUTPUT="$2"
      shift 2
      ;;
    --algorithms)
      IFS=',' read -r -a SELECTED_ALGORITHMS <<< "$2"
      shift 2
      ;;
    --datasets)
      IFS=',' read -r -a SELECTED_DATASETS <<< "$2"
      shift 2
      ;;
    --mode)
      MODE_SPECS+=("$2")
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ${#SELECTED_ALGORITHMS[@]} -eq 0 ]]; then
  SELECTED_ALGORITHMS=("${DEFAULT_ALGORITHMS[@]}")
fi

if [[ ${#SELECTED_DATASETS[@]} -eq 0 ]]; then
  SELECTED_DATASETS=("${DEFAULT_DATASETS[@]}")
fi

if [[ ${#MODE_SPECS[@]} -eq 0 ]]; then
  default_mode_specs
fi

for i in "${!SELECTED_ALGORITHMS[@]}"; do
  SELECTED_ALGORITHMS[$i]="$(canonical_algorithm "${SELECTED_ALGORITHMS[$i]}")"
done

for i in "${!SELECTED_DATASETS[@]}"; do
  SELECTED_DATASETS[$i]="$(canonical_dataset "${SELECTED_DATASETS[$i]}")"
done

if [[ -z "$CSV_OUTPUT" ]]; then
  CSV_OUTPUT="${OUTPUT_DIR}/chapter3_runtime_results.csv"
fi

if [[ "$DRY_RUN" == "false" && ! -x "$RUN_APP" ]]; then
  echo "run_app not found or not executable: $RUN_APP" >&2
  echo "Set --run-app to your build/run_app path." >&2
  exit 1
fi

if [[ ! -f "${SCRIPT_DIR}/summarize_tee_workers.py" ]]; then
  echo "Missing tee summarizer script: ${SCRIPT_DIR}/summarize_tee_workers.py" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "${OUTPUT_DIR}/logs"
printf 'algorithm,dataset,dataset_abbr,privacy_ratio,workers,mode,directed_flag,runtime_sec,total_tee_time_ms,total_tee_batches,total_secure_items,tee_summary_path,case_dir,log_path\n' > "$CSV_OUTPUT"

run_one_case() {
  local algorithm="$1"
  local dataset_name="$2"
  local dataset_abbr="$3"
  local dataset_dir="$4"
  local edge_file_name="$5"
  local vertex_prefix="$6"
  local dataset_directed="$7"
  local mode_name="$8"
  local mode_args_str="$9"

  local ratio_pct
  ratio_pct="$(ratio_to_percent "$PRIVACY_RATIO")"

  local efile="${dataset_dir}/${edge_file_name}"
  local vfile="${dataset_dir}/${vertex_prefix}-${PRIVACY_RATIO}.v"
  local algorithm_tag
  algorithm_tag="$(sanitize_name "$algorithm")"
  local dataset_tag
  dataset_tag="$(sanitize_name "$dataset_name")"
  local mode_tag
  mode_tag="$(sanitize_name "$mode_name")"
  local case_dir="${OUTPUT_DIR}/${algorithm_tag}/${dataset_tag}/ratio_${ratio_pct}/${mode_tag}"
  local tee_summary_path="${case_dir}/${algorithm}_tee_summary.tsv"
  local log_path="${OUTPUT_DIR}/logs/${algorithm_tag}_${dataset_tag}_${ratio_pct}_${mode_tag}.log"

  if [[ "$DRY_RUN" == "false" ]]; then
    if [[ ! -f "$efile" ]]; then
      echo "Missing edge file: $efile" >&2
      exit 1
    fi
    if [[ ! -f "$vfile" ]]; then
      echo "Missing vertex file: $vfile" >&2
      exit 1
    fi
  fi

  mkdir -p "$case_dir"

  local directed_flag
  directed_flag="$(effective_directed_flag "$algorithm" "$dataset_directed")"

  local cmd=(
    mpirun -n "$WORKERS" "$RUN_APP"
    --efile "$efile"
    --vfile "$vfile"
    --application "$algorithm"
    --out_prefix "$case_dir"
  )

  case "$algorithm" in
    sssp)
      cmd+=(--sssp_source "$SSSP_SOURCE")
      ;;
    bfs)
      cmd+=(--bfs_source "$BFS_SOURCE")
      ;;
    pagerank)
      cmd+=(--pr_d "$PR_D" --pr_mr "$PR_MR")
      ;;
  esac

  if [[ -n "$APP_CONCURRENCY" ]]; then
    cmd+=(--app_concurrency "$APP_CONCURRENCY")
  fi

  if [[ "$directed_flag" == "true" ]]; then
    cmd+=(--directed)
  fi

  if [[ -n "$mode_args_str" ]]; then
    local mode_args=()
    read -r -a mode_args <<< "$mode_args_str"
    cmd+=("${mode_args[@]}")
  fi

  printf '[CH3][%s][%s][%s%%][%s][w=%s] %s\n' \
    "$(printf '%s' "$algorithm" | tr '[:lower:]' '[:upper:]')" \
    "$dataset_name" "$ratio_pct" "$mode_name" "$WORKERS" "${cmd[*]}"

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if "${cmd[@]}" 2>&1 | tee "$log_path"; then
    local runtime_sec
    runtime_sec="$(awk '/- run algorithm:/ {print $(NF-1)}' "$log_path" | tail -n 1 | tr -d '\r')"
    if [[ -z "$runtime_sec" ]]; then
      echo "Failed to parse run algorithm time from $log_path" >&2
      exit 1
    fi

    python3 "${SCRIPT_DIR}/summarize_tee_workers.py" \
      --case-dir "$case_dir" \
      --app "$algorithm" \
      --summary "$tee_summary_path" > /dev/null

    local total_tee_time_ms
    local total_tee_batches
    local total_secure_items
    total_tee_time_ms="$(awk -F '\t' '$1 == "sum_total_tee_time_ms" {print $2}' "$tee_summary_path" | tr -d '\r')"
    total_tee_batches="$(awk -F '\t' '$1 == "sum_total_tee_batches" {print $2}' "$tee_summary_path" | tr -d '\r')"
    total_secure_items="$(awk -F '\t' '$1 == "sum_total_secure_items" {print $2}' "$tee_summary_path" | tr -d '\r')"

    if [[ -z "$total_tee_time_ms" ]]; then
      echo "Failed to parse total tee time from $tee_summary_path" >&2
      exit 1
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$algorithm" \
      "$dataset_name" \
      "$dataset_abbr" \
      "$PRIVACY_RATIO" \
      "$WORKERS" \
      "$mode_name" \
      "$directed_flag" \
      "$runtime_sec" \
      "$total_tee_time_ms" \
      "${total_tee_batches:-0}" \
      "${total_secure_items:-0}" \
      "$tee_summary_path" \
      "$case_dir" \
      "$log_path" >> "$CSV_OUTPUT"
  else
    echo "Command failed. Check log: $log_path" >&2
    exit 1
  fi
}

for spec in "${DATASET_SPECS[@]}"; do
  IFS='|' read -r dataset_name dataset_abbr dataset_dir edge_file_name vertex_prefix dataset_directed <<< "$spec"

  if ! contains_exact "$dataset_name" "${SELECTED_DATASETS[@]}"; then
    continue
  fi

  for algorithm in "${SELECTED_ALGORITHMS[@]}"; do
    for mode_spec in "${MODE_SPECS[@]}"; do
      mode_name="${mode_spec%%=*}"
      mode_args="${mode_spec#*=}"
      run_one_case "$algorithm" "$dataset_name" "$dataset_abbr" "$dataset_dir" \
        "$edge_file_name" "$vertex_prefix" "$dataset_directed" \
        "$mode_name" "$mode_args"
    done
  done
done

printf 'Results written to %s\n' "$CSV_OUTPUT"
