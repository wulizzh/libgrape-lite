#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_chapter2_partition_bench.sh [options]

Options:
  --run-app PATH          Path to the run_app binary. Default: ./run_app
  --workers N             Number of MPI workers. Default: 8
  --ratio VALUE           Privacy ratio suffix in vertex files. Default: 0.7
  --sssp-source ID        Source vertex for SSSP. Default: 0
  --bfs-source ID         Source vertex for BFS. Default: 0
  --app-concurrency N     Optional --app_concurrency forwarded to run_app
  --output-dir PATH       Base directory for logs and reports.
                          Default: ./benchmark_outputs/chapter2_partition
  --csv PATH              Output CSV path.
                          Default: <output-dir>/chapter2_partition_results.csv
  --algorithms LIST       Comma-separated app names.
                          Default: sssp,bfs,pagerank,wcc
  --datasets LIST         Comma-separated dataset names.
                          Default: web-Google,wiki-topcats,com-lj.ungraph
  --methods LIST          Comma-separated methods: Hash,Segmented,PESP
                          Default: Hash,Segmented,PESP
  --dry-run               Print commands without executing them.
  --help                  Show this help message.

Examples:
  ./run_chapter2_partition_bench.sh \
    --run-app /home/hust/codex_runs/libgrape-lite-privacy_v3-test/build/run_app

  ./run_chapter2_partition_bench.sh \
    --run-app ./run_app \
    --datasets web-Google,wiki-topcats \
    --algorithms sssp,bfs \
    --methods Hash,PESP
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_APP="${RUN_APP:-./run_app}"
WORKERS="${WORKERS:-8}"
PRIVACY_RATIO="${PRIVACY_RATIO:-0.7}"
SSSP_SOURCE="${SSSP_SOURCE:-0}"
BFS_SOURCE="${BFS_SOURCE:-0}"
APP_CONCURRENCY="${APP_CONCURRENCY:-}"
OUTPUT_DIR="${OUTPUT_DIR:-./benchmark_outputs/chapter2_partition}"
CSV_OUTPUT="${CSV_OUTPUT:-}"
DRY_RUN=false

DEFAULT_ALGORITHMS=("sssp" "bfs" "pagerank" "wcc")
DEFAULT_DATASETS=("web-Google" "wiki-topcats" "com-lj.ungraph")
DEFAULT_METHODS=("Hash" "Segmented" "PESP")

SELECTED_ALGORITHMS=()
SELECTED_DATASETS=()
SELECTED_METHODS=()

DATASET_SPECS=(
  "web-Google|GG|/home/hust/ljkdataset/web-google-ratio_dataset|web-Google.e|web-Google|true"
  "wiki-topcats|TC|/home/hust/ljkdataset/wiki-topcats-ratio_dataset|wiki-topcats.e|wiki-topcats|true"
  "com-lj.ungraph|LJ|/home/hust/ljkdataset/com-lj.ungraph-ratio_dataset|com-lj.ungraph.e|com-lj.ungraph|false"
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

canonical_method() {
  local raw="$1"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    hash)
      printf 'Hash\n'
      ;;
    segmented|seg)
      printf 'Segmented\n'
      ;;
    pesp|streaming|privacy-aware-streaming|privacy_aware_streaming)
      printf 'PESP\n'
      ;;
    *)
      return 1
      ;;
  esac
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

ratio_to_percent() {
  local ratio="$1"
  awk -v ratio="$ratio" 'BEGIN { printf "%d", ratio * 100 }'
}

sanitize_name() {
  local raw="$1"
  raw="${raw//[^A-Za-z0-9._-]/_}"
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
    --methods)
      IFS=',' read -r -a SELECTED_METHODS <<< "$2"
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

if [[ ${#SELECTED_METHODS[@]} -eq 0 ]]; then
  SELECTED_METHODS=("${DEFAULT_METHODS[@]}")
fi

for i in "${!SELECTED_ALGORITHMS[@]}"; do
  SELECTED_ALGORITHMS[$i]="$(canonical_algorithm "${SELECTED_ALGORITHMS[$i]}")"
done

for i in "${!SELECTED_METHODS[@]}"; do
  SELECTED_METHODS[$i]="$(canonical_method "${SELECTED_METHODS[$i]}")"
done

if [[ -z "$CSV_OUTPUT" ]]; then
  CSV_OUTPUT="${OUTPUT_DIR}/chapter2_partition_results.csv"
fi

if [[ "$DRY_RUN" == "false" && ! -x "$RUN_APP" ]]; then
  echo "run_app not found or not executable: $RUN_APP" >&2
  echo "Set --run-app to your build/run_app path." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "${OUTPUT_DIR}/logs"
printf 'algorithm,dataset,dataset_abbr,privacy_ratio,workers,method,directed_flag,runtime_sec,max_tee_time_ms,tee_summary_path,case_dir,log_path\n' > "$CSV_OUTPUT"

run_one_case() {
  local algorithm="$1"
  local dataset_name="$2"
  local dataset_abbr="$3"
  local dataset_dir="$4"
  local edge_file_name="$5"
  local vertex_prefix="$6"
  local dataset_directed="$7"
  local method="$8"

  local ratio_pct
  ratio_pct="$(ratio_to_percent "$PRIVACY_RATIO")"

  local efile="${dataset_dir}/${edge_file_name}"
  local vfile="${dataset_dir}/${vertex_prefix}-${PRIVACY_RATIO}.v"
  local algorithm_tag
  algorithm_tag="$(sanitize_name "$algorithm")"
  local dataset_tag
  dataset_tag="$(sanitize_name "$dataset_name")"
  local method_tag
  method_tag="$(sanitize_name "$method")"
  local case_dir="${OUTPUT_DIR}/${algorithm_tag}/${dataset_tag}/ratio_${ratio_pct}/${method_tag}"
  local report_dir="${case_dir}/report"
  local pesp_diag_dir="${case_dir}/pesp_diag"
  local tee_summary_path="${case_dir}/${algorithm}_tee_summary.tsv"
  local log_path="${OUTPUT_DIR}/logs/${algorithm_tag}_${dataset_tag}_${ratio_pct}_${method_tag}.log"

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

  mkdir -p "$case_dir" "$report_dir"

  local segmented_flag
  local pesp_flag
  case "$method" in
    Hash)
      segmented_flag="false"
      pesp_flag="false"
      ;;
    Segmented)
      segmented_flag="true"
      pesp_flag="false"
      ;;
    PESP)
      segmented_flag="true"
      pesp_flag="true"
      mkdir -p "$pesp_diag_dir"
      ;;
    *)
      echo "Unsupported method: $method" >&2
      exit 1
      ;;
  esac

  local directed_flag
  directed_flag="$(effective_directed_flag "$algorithm" "$dataset_directed")"

  local cmd=(
    mpirun -n "$WORKERS" "$RUN_APP"
    --efile "$efile"
    --vfile "$vfile"
    --application "$algorithm"
    --out_prefix "$case_dir"
    --segmented_partition="${segmented_flag}"
    --pesp_partition="${pesp_flag}"
    --partition_report=true
    --partition_output_prefix "$report_dir"
  )

  case "$algorithm" in
    sssp)
      cmd+=(--sssp_source "$SSSP_SOURCE")
      ;;
    bfs)
      cmd+=(--bfs_source "$BFS_SOURCE")
      ;;
  esac

  if [[ -n "$APP_CONCURRENCY" ]]; then
    cmd+=(--app_concurrency "$APP_CONCURRENCY")
  fi

  if [[ "$pesp_flag" == "true" ]]; then
    cmd+=(--pesp_output_prefix "$pesp_diag_dir")
  fi

  if [[ "$directed_flag" == "true" ]]; then
    cmd+=(--directed)
  fi

  printf '[%s][%s][%s%%][%s][w=%s] %s\n' \
    "$(printf '%s' "$algorithm" | tr '[:lower:]' '[:upper:]')" \
    "$dataset_name" "$ratio_pct" "$method" "$WORKERS" "${cmd[*]}"

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if "${cmd[@]}" 2>&1 | tee "$log_path"; then
    local runtime_sec
    runtime_sec="$(awk '/- run algorithm:/ {print $(NF-1)}' "$log_path" | tail -n 1)"
    if [[ -z "$runtime_sec" ]]; then
      echo "Failed to parse run algorithm time from $log_path" >&2
      exit 1
    fi

    python3 "${SCRIPT_DIR}/summarize_tee_workers.py" \
      --case-dir "$case_dir" \
      --app "$algorithm" \
      --summary "$tee_summary_path" > /dev/null

    local max_tee_time_ms
    max_tee_time_ms="$(awk -F '\t' '$1 == "max_total_tee_time_ms" {print $2}' "$tee_summary_path")"
    if [[ -z "$max_tee_time_ms" ]]; then
      echo "Failed to parse max tee time from $tee_summary_path" >&2
      exit 1
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$algorithm" \
      "$dataset_name" \
      "$dataset_abbr" \
      "$PRIVACY_RATIO" \
      "$WORKERS" \
      "$method" \
      "$directed_flag" \
      "$runtime_sec" \
      "$max_tee_time_ms" \
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
    for method in "${SELECTED_METHODS[@]}"; do
      run_one_case "$algorithm" "$dataset_name" "$dataset_abbr" "$dataset_dir" \
        "$edge_file_name" "$vertex_prefix" "$dataset_directed" "$method"
    done
  done
done

printf 'Results written to %s\n' "$CSV_OUTPUT"
