#!/usr/bin/env bash
# Create the BigQuery datasets and empty tables HERMES needs in a fresh Google
# Cloud project. M-Lab's own deployment (mlab-collaboration) already has all of
# them, so this script refuses to target it.
#
# Usage:
#   scripts/init_project.sh <project> [--dry-run]
#   HERMES_PROJECT=<project> scripts/init_project.sh [--dry-run]
#
# Idempotent: existing datasets and tables are left untouched, so it is safe to
# re-run after a partial failure. Creating empty tables bills nothing; loading
# the small hermes.site_to_state seed is a free load job.
#
# Table definitions live in scripts/schemas/: tables.tsv (partitioning,
# clustering, how each table is populated) plus one <dataset>.<table>.json
# schema per table, exported from the live mlab-collaboration tables.
# See docs/initial_configuration.md for what to run afterwards.
set -euo pipefail

# The datasets must share a location with M-Lab's measurement-lab datasets (US
# multi-region): BigQuery cannot join tables across locations.
LOCATION=US
DATASETS=(hermes ix_data hermes_union)

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
schemas="$here/schemas"

project="${HERMES_PROJECT:-}"
dry_run=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) dry_run=1 ;;
    -h | --help)
      sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "unknown option: $arg" >&2
      exit 2
      ;;
    *) project="$arg" ;;
  esac
done

if [[ -z "$project" ]]; then
  echo "usage: $0 <project> [--dry-run]   (or set HERMES_PROJECT)" >&2
  exit 2
fi
if [[ "$project" == "mlab-collaboration" ]]; then
  echo "refusing: mlab-collaboration is M-Lab's deployment and is already configured" >&2
  exit 2
fi
if ! command -v bq >/dev/null; then
  echo "bq not found: install the Google Cloud SDK and run 'gcloud auth login'" >&2
  exit 1
fi

run() {
  if ((dry_run)); then
    printf '[dry-run]'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

exists() { bq --project_id="$project" show "$1" >/dev/null 2>&1; }

for ds in "${DATASETS[@]}"; do
  if exists "$project:$ds"; then
    echo "exists   dataset $project:$ds"
  else
    echo "create   dataset $project:$ds"
    run bq --project_id="$project" --location="$LOCATION" mk --dataset "$project:$ds"
  fi
done

# Columns: dataset, table, partition_field, clustering, require_partition_filter,
# populated_by. "-" marks an empty value so tab-splitting never collapses fields.
while IFS=$'\t' read -r ds table part_field clustering require_filter _populated_by; do
  [[ -z "$ds" || "$ds" == \#* ]] && continue
  ref="$project:$ds.$table"
  schema="$schemas/$ds.$table.json"
  if [[ ! -f "$schema" ]]; then
    echo "missing schema file: $schema" >&2
    exit 1
  fi
  if exists "$ref"; then
    echo "exists   table   $ref"
    continue
  fi
  args=(mk --table)
  if [[ "$part_field" != "-" ]]; then
    args+=(--time_partitioning_field="$part_field" --time_partitioning_type=DAY)
  fi
  if [[ "$clustering" != "-" ]]; then
    args+=(--clustering_fields="$clustering")
  fi
  if [[ "$require_filter" == "true" ]]; then
    args+=(--require_partition_filter)
  fi
  echo "create   table   $ref"
  run bq --project_id="$project" "${args[@]}" "$ref" "$schema"
done <"$schemas/tables.tsv"

# hermes.site_to_state is static (M-Lab site -> city/state); ship its contents.
seed="$schemas/data/hermes.site_to_state.csv"
site_ref="$project:hermes.site_to_state"
rows="$(bq --project_id="$project" query --nouse_legacy_sql --format=csv --quiet \
  "SELECT COUNT(*) FROM \`$project.hermes.site_to_state\`" 2>/dev/null | tail -n 1 || true)"
if [[ "$rows" =~ ^[1-9][0-9]*$ ]]; then
  echo "exists   rows    $site_ref ($rows rows)"
else
  echo "load     rows    $site_ref <- ${seed#"$here"/}"
  run bq --project_id="$project" load --source_format=CSV --skip_leading_rows=1 \
    "$site_ref" "$seed"
fi

cat <<EOF

Done. Tables are created empty. Next (docs/initial_configuration.md):
  export HERMES_PROJECT=$project
  PYTHONPATH=src python -m hermes.pipeline.bootstrap_tables
  PYTHONPATH=src python -m hermes.pipeline.refresh_topology_tables --date <YYYY-MM-DD> --ip-version both
  python -m cleaning_polygons.build --build-version <version> --upload   # see cleaning_polygons/README.md
EOF
