#!/usr/bin/env bash
# Replay the ETL state machine over the last N days, one at a time.
# Running them all at once would spin up N concurrent Glue jobs and a bill.
set -euo pipefail

DAYS="${1:-14}"
REGION="${REGION:-ca-central-1}"
SM_ARN="$(cd infra && terraform output -raw state_machine_arn)"

for i in $(seq 1 "${DAYS}"); do
  if date -u -d "-${i} day" +%Y-%m-%d >/dev/null 2>&1; then
    RUN_DATE="$(date -u -d "-${i} day" +%Y-%m-%d)"      # GNU date
  else
    RUN_DATE="$(date -u -v-"${i}"d +%Y-%m-%d)"          # BSD/macOS date
  fi

  echo "starting backfill for ${RUN_DATE}"
  aws stepfunctions start-execution \
    --state-machine-arn "${SM_ARN}" \
    --name "backfill-${RUN_DATE}-$(date +%s)" \
    --input "{\"run_date\":\"${RUN_DATE}\"}" \
    --region "${REGION}" >/dev/null

  sleep 120
done

echo "backfill submitted for ${DAYS} days"
