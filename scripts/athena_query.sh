#!/usr/bin/env bash
# Run an Athena query and download its result CSV under a name you choose.
#
# Athena writes every result to s3://<athena bucket>/results/<uuid>.csv, which is
# tedious to match back to the query that produced it. This resolves the location
# from the execution id and saves it as data/processed/<name>.csv.
#
#   ./scripts/athena_query.sh delay_by_hour <<'SQL'
#   SELECT hour_of_day, count(*) AS events
#   FROM transitpulse.stop_events
#   GROUP BY hour_of_day ORDER BY hour_of_day;
#   SQL
#
#   ./scripts/athena_query.sh baselines sql/04_baselines.sql
set -uo pipefail

REGION="${REGION:-ca-central-1}"
WORKGROUP="${WORKGROUP:-transitpulse}"
OUT_DIR="${OUT_DIR:-data/processed}"

NAME="${1:-}"
if [ -z "$NAME" ]; then
  echo "usage: $0 <name> [sql-file]     (SQL on stdin if no file given)" >&2
  exit 1
fi

if [ -n "${2:-}" ]; then
  [ -f "$2" ] || { echo "no such file: $2" >&2; exit 1; }
  SQL="$(cat "$2")"
else
  SQL="$(cat)"
fi

[ -z "${SQL// }" ] && { echo "empty query" >&2; exit 1; }

mkdir -p "$OUT_DIR"

QID="$(aws athena start-query-execution \
        --query-string "$SQL" \
        --work-group "$WORKGROUP" \
        --region "$REGION" \
        --query QueryExecutionId --output text)" || exit 1

printf "query %s " "$QID"

for _ in $(seq 1 90); do
  STATE="$(aws athena get-query-execution --query-execution-id "$QID" \
            --region "$REGION" --query "QueryExecution.Status.State" --output text)"
  case "$STATE" in
    SUCCEEDED) break ;;
    FAILED|CANCELLED)
      echo "-> $STATE"
      aws athena get-query-execution --query-execution-id "$QID" --region "$REGION" \
        --query "QueryExecution.Status.StateChangeReason" --output text >&2
      exit 1 ;;
  esac
  printf "."
  sleep 2
done

if [ "$STATE" != "SUCCEEDED" ]; then
  echo " -> timed out (still $STATE)" >&2
  exit 1
fi

LOC="$(aws athena get-query-execution --query-execution-id "$QID" --region "$REGION" \
        --query "QueryExecution.ResultConfiguration.OutputLocation" --output text)"

aws s3 cp "$LOC" "${OUT_DIR}/${NAME}.csv" --region "$REGION" >/dev/null || exit 1

SCANNED="$(aws athena get-query-execution --query-execution-id "$QID" --region "$REGION" \
            --query "QueryExecution.Statistics.DataScannedInBytes" --output text)"

echo " -> ${OUT_DIR}/${NAME}.csv  ($(( SCANNED / 1024 / 1024 )) MB scanned)"
