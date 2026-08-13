#!/usr/bin/env bash
# Daily data check: is the silver layer accumulating usable training days?
#
# scripts/daily_check.sh answers "are the services healthy". This answers the
# different question of "is the data any good", which is what actually gates
# Phase 5 -- a green pipeline can still produce thin or unlabelled days.
set -uo pipefail

REGION="${REGION:-ca-central-1}"
WORKGROUP="${WORKGROUP:-transitpulse}"

# Phase 5 splits train/val/test by time (<=T-14d, T-14d..T-7d, >T-7d), so
# anything under ~21 days leaves the training window empty.
TARGET_DAYS=21

run_query() {
  local sql="$1"
  local qid
  qid="$(aws athena start-query-execution \
          --query-string "$sql" \
          --work-group "$WORKGROUP" \
          --region "$REGION" \
          --query QueryExecutionId --output text 2>/dev/null)" || return 1
  [ -z "$qid" ] && return 1

  local state
  for _ in $(seq 1 60); do
    state="$(aws athena get-query-execution --query-execution-id "$qid" \
              --region "$REGION" --query "QueryExecution.Status.State" --output text)"
    case "$state" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$qid" --region "$REGION" \
          --query "QueryExecution.Status.StateChangeReason" --output text
        return 1 ;;
    esac
    sleep 2
  done
  [ "$state" != "SUCCEEDED" ] && { echo "query timed out"; return 1; }

  aws athena get-query-results --query-execution-id "$qid" --region "$REGION" \
    --query "ResultSet.Rows[].Data[].VarCharValue" --output text
}

echo "=============================================="
echo " TransitPulse data check"
echo "=============================================="
echo

echo "--- collection progress ---"
DAYS="$(run_query "SELECT count(DISTINCT service_date) FROM transitpulse.stop_events" \
        | awk '{print $NF}')"
if [ -n "${DAYS:-}" ] && [ "$DAYS" -eq "$DAYS" ] 2>/dev/null; then
  printf "  %s of %s days collected" "$DAYS" "$TARGET_DAYS"
  if [ "$DAYS" -ge "$TARGET_DAYS" ]; then
    echo "   -- enough for a time-based split. Phase 5 is unblocked."
  else
    echo "   -- $(( TARGET_DAYS - DAYS )) to go"
  fi
else
  echo "  could not read stop_events (has the ETL run yet?)"
fi
echo

echo "--- last 10 service days ---"
echo "  date        events   labelled  label_rate  dupes"
run_query "
  SELECT
    cast(service_date AS varchar),
    cast(count(*) AS varchar),
    cast(count(observed_delay_sec) AS varchar),
    cast(round(count(observed_delay_sec) * 1.0 / count(*), 3) AS varchar),
    cast(count(*) - count(DISTINCT trip_id || '|' || stop_id) AS varchar)
  FROM transitpulse.stop_events
  GROUP BY service_date
  ORDER BY service_date DESC
  LIMIT 10" \
| tr '\t' '\n' | tail -n +6 | xargs -n5 printf "  %-11s %8s %9s %11s %6s\n"

echo
echo "What to look for:"
echo "  events      ~300k-600k on a full day. A day at 10% of normal means the"
echo "              poller was down -- exclude it rather than training on it."
echo "  label_rate  >= 0.85. Below that the DQ gate would have quarantined it."
echo "  dupes       MUST be 0. Non-zero means the silver window key is wrong."
