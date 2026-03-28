#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_cdlp_partition_bench.sh [options]

Options:
  --run-app PATH       Path to the run_app binary. Default: ./run_app
  --workers N          Number of MPI workers. Default: 6
  --rounds N           CDLP max rounds. Default: 10
  --output-dir PATH    Base directory for logs and reports.
                       Default: ./benchmark_outputs/cdlp_partition
  --csv PATH           Output CSV path.
                       Default: <output-dir>/cdlp_partition_results.csv
  --datasets LIST      Comma-separated dataset names.
                       Default: web-Google,wiki-topcats,com-lj.ungraph
  --ratios LIST        Comma-separated privacy ratios matching vertex files.
                       Default: 0.1,0.3,0.5,0.7,0.9
  --methods LIST       Comma-separated methods: Hash,Segmented,PESP
                       Default: Hash,Segmented,PESP
  --dry-run            Print commands without executing them.
  --help               Show this help message.

Examples:
  ./run_cdlp_partition_bench.sh \
    --run-app /home/hust/libgrape-lite-privacy_v2/build/run_app

  ./run_cdlp_partition_bench.sh \
    --run-app ./run_app \
    --datasets web-Google \
    --ratios 0.1,0.3 \
    --methods Hash,PESP
EOF
}

RUN_APP="${RUN_APP:-./run_app}"
WORKERS="${WORKERS:-6}"
CDLP_ROUNDS="${CDLP_ROUNDS:-10}"
OUTPUT_DIR="${OUTPUT_DIR:-./benchmark_outputs/cdlp_partition}"
CSV_OUTPUT="${CSV_OUTPUT:-}"
DRY_RUN=false

DEFAULT_DATASETS=("web-Google" "wiki-topcats" "com-lj.ungraph")
DEFAULT_RATIOS=("0.1" "0.3" "0.5" "0.7" "0.9")
DEFAULT_METHODS=("Hash" "Segmented" "PESP")

SELECTED_DATASETS=()
SELECTED_RATIOS=()
SELECTED_METHODS=()

DATASET_SPECS=(
  "web-Google|/home/hust/ljkdataset/web-google-ratio_dataset|web-Google.e|web-Google|true"
  "wiki-topcats|/home/hust/ljkdataset/wiki-topcats-ratio_dataset|wiki-topcats.e|wiki-topcats|true"
  "com-lj.ungraph|/home/hust/ljkdataset/com-lj.ungraph-ratio_dataset|com-lj.ungraph.e|com-lj.ungraph|false"
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
    pesp)
      printf 'PESP\n'
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
    --rounds)
      CDLP_ROUNDS="$2"
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
    --datasets)
      IFS=',' read -r -a SELECTED_DATASETS <<< "$2"
      shift 2
      ;;
    --ratios)
      IFS=',' read -r -a SELECTED_RATIOS <<< "$2"
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

if [[ ${#SELECTED_DATASETS[@]} -eq 0 ]]; then
  SELECTED_DATASETS=("${DEFAULT_DATASETS[@]}")
fi

if [[ ${#SELECTED_RATIOS[@]} -eq 0 ]]; then
  SELECTED_RATIOS=("${DEFAULT_RATIOS[@]}")
fi

if [[ ${#SELECTED_METHODS[@]} -eq 0 ]]; then
  SELECTED_METHODS=("${DEFAULT_METHODS[@]}")
fi

for i in "${!SELECTED_METHODS[@]}"; do
  SELECTED_METHODS[$i]="$(canonical_method "${SELECTED_METHODS[$i]}")"
done

if [[ -z "$CSV_OUTPUT" ]]; then
  CSV_OUTPUT="${OUTPUT_DIR}/cdlp_partition_results.csv"
fi

if [[ "$DRY_RUN" == "false" && ! -x "$RUN_APP" ]]; then
  echo "run_app not found or not executable: $RUN_APP" >&2
  echo "Set --run-app to your build/run_app path." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "${OUTPUT_DIR}/logs"
printf 'algorithm,dataset,privacy_ratio,method,time\n' > "$CSV_OUTPUT"

run_one_case() {
  local dataset_name="$1"
  local dataset_dir="$2"
  local edge_file_name="$3"
  local vertex_prefix="$4"
  local directed="$5"
  local ratio="$6"
  local method="$7"

  local ratio_pct
  ratio_pct="$(ratio_to_percent "$ratio")"

  local efile="${dataset_dir}/${edge_file_name}"
  local vfile="${dataset_dir}/${vertex_prefix}-${ratio}.v"
  local dataset_tag
  dataset_tag="$(sanitize_name "$dataset_name")"
  local method_tag
  method_tag="$(sanitize_name "$method")"
  local case_dir="${OUTPUT_DIR}/${dataset_tag}/ratio_${ratio_pct}/${method_tag}"
  local report_dir="${case_dir}/report"
  local pesp_diag_dir="${case_dir}/pesp_diag"
  local log_path="${OUTPUT_DIR}/logs/CDLP_${dataset_tag}_${ratio_pct}_${method_tag}.log"

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

  local cmd=(
    mpirun -n "$WORKERS" "$RUN_APP"
    --efile "$efile"
    --vfile "$vfile"
    --application cdlp
    --cdlp_mr "$CDLP_ROUNDS"
    --out_prefix "$case_dir"
    --segmented_partition="${segmented_flag}"
    --pesp_partition="${pesp_flag}"
    --partition_report=true
    --partition_output_prefix "$report_dir"
  )

  if [[ "$pesp_flag" == "true" ]]; then
    cmd+=(--pesp_output_prefix "$pesp_diag_dir")
  fi

  if [[ "$directed" == "true" ]]; then
    cmd+=(--directed)
  fi

  printf '[CDLP][%s][%s%%][%s] %s\n' \
    "$dataset_name" "$ratio_pct" "$method" "${cmd[*]}"

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if "${cmd[@]}" 2>&1 | tee "$log_path"; then
    local time_sec
    time_sec="$(awk '/- run algorithm:/ {print $(NF-1)}' "$log_path" | tail -n 1)"
    if [[ -z "$time_sec" ]]; then
      echo "Failed to parse run algorithm time from $log_path" >&2
      exit 1
    fi
    printf 'CDLP,%s,%s,%s,%ss\n' \
      "$dataset_name" "$ratio_pct" "$method" "$time_sec" >> "$CSV_OUTPUT"
  else
    echo "Command failed. Check log: $log_path" >&2
    exit 1
  fi
}

for spec in "${DATASET_SPECS[@]}"; do
  IFS='|' read -r dataset_name dataset_dir edge_file_name vertex_prefix directed <<< "$spec"

  if ! contains_exact "$dataset_name" "${SELECTED_DATASETS[@]}"; then
    continue
  fi

  for ratio in "${SELECTED_RATIOS[@]}"; do
    for method in "${SELECTED_METHODS[@]}"; do
      run_one_case "$dataset_name" "$dataset_dir" "$edge_file_name" \
        "$vertex_prefix" "$directed" "$ratio" "$method"
    done
  done
done

printf 'Results written to %s\n' "$CSV_OUTPUT"
