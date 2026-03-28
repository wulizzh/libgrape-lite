#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_sssp_runtime_bench.sh [options]

Options:
  --run-app PATH         Path to the run_app binary. Default: ./run_app
  --workers N            Number of MPI workers. Default: 6
  --source ID            SSSP source vertex id. Default: 1
  --dataset NAME         Dataset label written to output CSV. Default: custom
  --dataset-dir PATH     Directory containing edge file and ratio-specific vertex files.
  --edge-file NAME       Edge file name under dataset-dir.
  --vertex-prefix NAME   Vertex file prefix. Runtime script will load
                         <dataset-dir>/<vertex-prefix>-<ratio>.v
  --ratios LIST          Comma-separated privacy ratios. Default: 0.7,0.9
  --directed BOOL        true/false. Default: true
  --output-dir PATH      Base directory for runtime logs and summaries.
                         Default: ./benchmark_outputs/sssp_runtime
  --csv PATH             Output CSV path.
                         Default: <output-dir>/sssp_runtime_results.csv
  --mode NAME=ARGS       Repeatable. Defines one experiment mode and its extra flags.
                         Example:
                         --mode 'Static=--segmented_partition=true --pesp_partition=true'
                         --mode 'Runtime=--segmented_partition=true --pesp_partition=true --runtime_feedback=true'
  --dry-run              Print commands without executing them.
  --help                 Show this help message.

This script is intended for third-chapter runtime experiments. It expects the
application to emit sssp_runtime_worker_*.tsv files under each case out_prefix.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUMMARIZER="${SCRIPT_DIR}/summarize_sssp_runtime.py"

RUN_APP="${RUN_APP:-./run_app}"
WORKERS="${WORKERS:-6}"
SSSP_SOURCE="${SSSP_SOURCE:-1}"
DATASET_NAME="${DATASET_NAME:-custom}"
DATASET_DIR="${DATASET_DIR:-}"
EDGE_FILE_NAME="${EDGE_FILE_NAME:-}"
VERTEX_PREFIX="${VERTEX_PREFIX:-}"
RATIOS_CSV="${RATIOS_CSV:-0.7,0.9}"
DIRECTED="${DIRECTED:-true}"
OUTPUT_DIR="${OUTPUT_DIR:-./benchmark_outputs/sssp_runtime}"
CSV_OUTPUT="${CSV_OUTPUT:-}"
DRY_RUN=false

MODE_SPECS=()

ratio_to_percent() {
  local ratio="$1"
  awk -v ratio="$ratio" 'BEGIN { printf "%d", ratio * 100 }'
}

sanitize_name() {
  local raw="$1"
  raw="${raw//[^A-Za-z0-9._-]/_}"
  printf '%s\n' "$raw"
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
    --source)
      SSSP_SOURCE="$2"
      shift 2
      ;;
    --dataset)
      DATASET_NAME="$2"
      shift 2
      ;;
    --dataset-dir)
      DATASET_DIR="$2"
      shift 2
      ;;
    --edge-file)
      EDGE_FILE_NAME="$2"
      shift 2
      ;;
    --vertex-prefix)
      VERTEX_PREFIX="$2"
      shift 2
      ;;
    --ratios)
      RATIOS_CSV="$2"
      shift 2
      ;;
    --directed)
      DIRECTED="$2"
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

if [[ -z "$DATASET_DIR" || -z "$EDGE_FILE_NAME" || -z "$VERTEX_PREFIX" ]]; then
  echo "You must provide --dataset-dir, --edge-file, and --vertex-prefix." >&2
  exit 1
fi

if [[ ${#MODE_SPECS[@]} -eq 0 ]]; then
  MODE_SPECS+=("Static=--segmented_partition=true --pesp_partition=true")
fi

if [[ -z "$CSV_OUTPUT" ]]; then
  CSV_OUTPUT="${OUTPUT_DIR}/sssp_runtime_results.csv"
fi

if [[ "$DRY_RUN" == "false" && ! -x "$RUN_APP" ]]; then
  echo "run_app not found or not executable: $RUN_APP" >&2
  echo "Set --run-app to your build/run_app path." >&2
  exit 1
fi

if [[ ! -f "$SUMMARIZER" ]]; then
  echo "Missing summarizer script: $SUMMARIZER" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "${OUTPUT_DIR}/logs"
printf 'algorithm,dataset,privacy_ratio,mode,run_time_sec,peak_round_duration_ms,peak_imbalance_max_over_avg,peak_active_private_vertices,peak_private_candidates,peak_secure_memory_level,trigger_rounds,migration_count,avg_wait_time_ms,feedback_overhead_ms,coscheduling_rounds\n' > "$CSV_OUTPUT"

IFS=',' read -r -a RATIOS <<< "$RATIOS_CSV"

run_one_case() {
  local ratio="$1"
  local mode_name="$2"
  local mode_args_str="$3"

  local ratio_pct
  ratio_pct="$(ratio_to_percent "$ratio")"

  local efile="${DATASET_DIR}/${EDGE_FILE_NAME}"
  local vfile="${DATASET_DIR}/${VERTEX_PREFIX}-${ratio}.v"
  local dataset_tag
  dataset_tag="$(sanitize_name "$DATASET_NAME")"
  local mode_tag
  mode_tag="$(sanitize_name "$mode_name")"
  local case_dir="${OUTPUT_DIR}/${dataset_tag}/ratio_${ratio_pct}/${mode_tag}"
  local log_path="${OUTPUT_DIR}/logs/SSSP_RUNTIME_${dataset_tag}_${ratio_pct}_${mode_tag}.log"

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

  local cmd=(
    mpirun -n "$WORKERS" "$RUN_APP"
    --efile "$efile"
    --vfile "$vfile"
    --application sssp
    --sssp_source "$SSSP_SOURCE"
    --out_prefix "$case_dir"
  )

  if [[ "$DIRECTED" == "true" ]]; then
    cmd+=(--directed)
  fi

  if [[ -n "$mode_args_str" ]]; then
    local mode_args=()
    read -r -a mode_args <<< "$mode_args_str"
    cmd+=("${mode_args[@]}")
  fi

  printf '[SSSP-RUNTIME][%s][%s%%][%s] %s\n' \
    "$DATASET_NAME" "$ratio_pct" "$mode_name" "${cmd[*]}"

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if "${cmd[@]}" 2>&1 | tee "$log_path"; then
    python3 "$SUMMARIZER" "$case_dir"

    local run_time_sec
    run_time_sec="$(awk '/- run algorithm:/ {print $(NF-1)}' "$log_path" | tail -n 1)"
    if [[ -z "$run_time_sec" ]]; then
      echo "Failed to parse run algorithm time from $log_path" >&2
      exit 1
    fi

    local peak_round_ms
    local peak_imbalance
    local peak_active_private_vertices
    local peak_private_candidates
    local peak_secure_memory_level
    local trigger_rounds
    local migration_count
    local avg_wait_time_ms
    local feedback_overhead_ms
    local coscheduling_rounds

    peak_round_ms="$(awk -F '\t' '$1=="peak_round_duration_ms" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    peak_imbalance="$(awk -F '\t' '$1=="peak_imbalance_max_over_avg" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    peak_active_private_vertices="$(awk -F '\t' '$1=="peak_active_private_vertices" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    peak_private_candidates="$(awk -F '\t' '$1=="peak_private_candidates" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    peak_secure_memory_level="$(awk -F '\t' '$1=="peak_secure_memory_level" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    trigger_rounds="$(awk -F '\t' '$1=="trigger_rounds" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    migration_count="$(awk -F '\t' '$1=="migration_count" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    avg_wait_time_ms="$(awk -F '\t' '$1=="avg_wait_time_ms" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    feedback_overhead_ms="$(awk -F '\t' '$1=="feedback_overhead_ms" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"
    coscheduling_rounds="$(awk -F '\t' '$1=="coscheduling_rounds" {print $2}' "${case_dir}/sssp_runtime_case_summary.tsv")"

    printf 'SSSP,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$DATASET_NAME" "$ratio_pct" "$mode_name" "$run_time_sec" \
      "${peak_round_ms:-0}" "${peak_imbalance:-0}" \
      "${peak_active_private_vertices:-0}" "${peak_private_candidates:-0}" \
      "${peak_secure_memory_level:-0}" "${trigger_rounds:-0}" \
      "${migration_count:-0}" "${avg_wait_time_ms:-0}" \
      "${feedback_overhead_ms:-0}" "${coscheduling_rounds:-0}" \
      >> "$CSV_OUTPUT"
  else
    echo "Command failed. Check log: $log_path" >&2
    exit 1
  fi
}

for ratio in "${RATIOS[@]}"; do
  for mode_spec in "${MODE_SPECS[@]}"; do
    mode_name="${mode_spec%%=*}"
    mode_args="${mode_spec#*=}"
    run_one_case "$ratio" "$mode_name" "$mode_args"
  done
done

printf 'Results written to %s\n' "$CSV_OUTPUT"
