#!/usr/bin/env bash
# Five-minute daily health check while the pipeline is collecting data.
set -uo pipefail

REGION="${REGION:-ca-central-1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
TODAY="$(date -u +%Y-%m-%d)"

echo "=== 1. bronze arriving today? ==="
aws s3 ls "s3://transitpulse-bronze-${ACCT}/raw/trip_updates/dt=${TODAY}/" \
  --recursive --region "${REGION}" | tail -3

echo "=== 2. firehose conversion errors (want 0) ==="
aws s3 ls "s3://transitpulse-bronze-${ACCT}/errors/" --recursive --region "${REGION}" | wc -l

echo "=== 3. alarms currently firing ==="
aws cloudwatch describe-alarms --state-value ALARM --region "${REGION}" \
  --query "MetricAlarms[].AlarmName" --output table

echo "=== 4. poller dead letter queue ==="
DLQ_URL="$(aws sqs get-queue-url --queue-name transitpulse-poller-dlq \
  --region "${REGION}" --query QueueUrl --output text 2>/dev/null)"
if [ -n "${DLQ_URL}" ] && [ "${DLQ_URL}" != "None" ]; then
  aws sqs get-queue-attributes --queue-url "${DLQ_URL}" \
    --attribute-names ApproximateNumberOfMessages --region "${REGION}" \
    --query "Attributes.ApproximateNumberOfMessages" --output text
fi

echo "=== 5. yesterday's spend on this project ==="
if date -u -d "-2 day" +%Y-%m-%d >/dev/null 2>&1; then
  START="$(date -u -d '-2 day' +%Y-%m-%d)"
else
  START="$(date -u -v-2d +%Y-%m-%d)"
fi
aws ce get-cost-and-usage \
  --time-period "Start=${START},End=${TODAY}" \
  --granularity DAILY --metrics UnblendedCost \
  --filter '{"Tags":{"Key":"Project","Values":["transitpulse"]}}' \
  --query "ResultsByTime[].Total.UnblendedCost.Amount" --output text 2>/dev/null \
  || echo "(cost data lags ~24h on new accounts)"

# A missing execution is worse than a failed one: failure emails you, absence is
# silent. Resolved by name rather than terraform output so this runs from
# anywhere and does not need an initialised working directory.
echo
echo "=== 6. last night's ETL (want a new SUCCEEDED each day, ~02:20 UTC) ==="
SM_ARN="$(aws stepfunctions list-state-machines --region "${REGION}" \
  --query "stateMachines[?name=='transitpulse-etl'].stateMachineArn" \
  --output text 2>/dev/null)"
if [ -n "${SM_ARN}" ] && [ "${SM_ARN}" != "None" ]; then
  aws stepfunctions list-executions --state-machine-arn "${SM_ARN}" \
    --max-items 3 --region "${REGION}" \
    --query "executions[].{Status:status,Started:startDate}" --output table
else
  echo "state machine 'transitpulse-etl' not found -- check the region"
fi
