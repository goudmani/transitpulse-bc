# Daily runbook

What to run each day while the pipeline collects data. Roughly 3 minutes.

Full paths throughout, so nothing depends on shell aliases being loaded.

---

## 1. Start a session

Paste this block. It works in any fresh terminal.

```bash
cd ~/mds_ubc/transitpulse-bc
conda activate /Users/manikanthgoudgurujala/miniforge3/envs/transitpulse
export REGION=ca-central-1
export AWS_DEFAULT_REGION=ca-central-1
export ACCT=$(aws sts get-caller-identity --query Account --output text)
echo "account=$ACCT region=$REGION  $(python --version)"
```

Expect your 12-digit account number and Python 3.12.x.

**If `aws sts` fails:** credentials are in `~/.aws/credentials` and don't expire, so
this should not happen. If it does, re-run `aws configure`.

---

## 2. Health check

```bash
make check
```

Five checks. What good looks like:

| Check | Good |
|---|---|
| 1. Bronze arriving today | newest file within ~10 minutes |
| 2. Firehose conversion errors | **0** |
| 3. Alarms firing | empty table |
| 4. Poller dead letter queue | **0** |
| 5. Yesterday's spend | ~0.70 |

Check 5 prints `(cost data lags ~24h on new accounts)` until cost allocation tags
have been active for a day. Harmless.

---

## 3. Did last night's ETL run?

This is **check 6 of `make check`** — no separate command needed. The state
machine fires at **02:20 UTC** (19:20 PDT the previous evening), so a healthy
morning shows a new `SUCCEEDED` execution dated today.

**A missing execution is worse than a failed one** — failure alerts you by email,
absence is silent. Three weeks of that leaves a hole in the training data you
cannot fill, because bronze expires at 30 days.

To re-run a day the scheduler missed:

```bash
SM=$(terraform -chdir=infra output -raw state_machine_arn)
aws stepfunctions start-execution --state-machine-arn $SM \
  --input '{"run_date":"2026-08-15"}'
```

An explicit `YYYY-MM-DD` processes exactly that day. The scheduler passes a full
ISO timestamp instead, which `resolve_run_date()` shifts back to the previous day.

---

## 4. Is data actually accumulating?

Once a week is enough. In Athena, workgroup `transitpulse`:

```sql
SELECT service_date, count(*) AS events,
       count(observed_delay_sec) AS labelled
FROM transitpulse.stop_events
GROUP BY service_date ORDER BY service_date;
```

Expect roughly **300,000–600,000 events per full day**. A day at 10% of normal
volume means the poller was down — exclude it from training rather than training
on it.

---

## When something is wrong

### No new bronze files

```bash
aws logs tail /aws/lambda/transitpulse-poller --since 30m | grep records_ingested | tail -3
```

- **Nothing at all** → the schedule is disabled. `make resume`
- **`"value": 0`** → TransLink's feed is empty. Usually theirs, not yours. Check with:
  ```bash
  python - <<'PY'
  import os, requests
  from google.transit import gtfs_realtime_pb2
  r = requests.get("https://gtfsapi.translink.ca/v3/gtfsrealtime",
                   params={"apikey": os.environ["TL_KEY"]}, timeout=15)
  m = gtfs_realtime_pb2.FeedMessage(); m.ParseFromString(r.content)
  print("HTTP", r.status_code, "entities:", len(m.entity))
  PY
  ```
  (needs `export TL_KEY="$(grep TRANSLINKAPIKEY .env | cut -d= -f2)"` first)
- **Errors in the log** → read the traceback

### An alarm is firing

```bash
aws cloudwatch describe-alarms --state-value ALARM \
  --query "MetricAlarms[].{Name:AlarmName,Reason:StateReason}" --output table
```

| Alarm | Means |
|---|---|
| `ingest-stalled` | <1000 records in 15 min — poller down or feed empty |
| `poller-errors` | Lambda throwing. Check logs. |
| `poller-dlq-not-empty` | Polls lost entirely. Alarm stays lit until the queue is purged. |
| `kinesis-iterator-age` | A consumer is falling behind |
| `estimated-charges` | Spend over threshold — ingestion auto-disabled |

Purge the DLQ after fixing the cause:
```bash
aws sqs purge-queue --queue-url $(aws sqs get-queue-url --queue-name transitpulse-poller-dlq --query QueueUrl --output text)
```

### The ETL failed

```bash
aws glue get-job-runs --job-name transitpulse-silver \
  --query "JobRuns[0].{State:JobRunState,Err:ErrorMessage}"
```

Errors read far better in the console: **Glue → ETL jobs → the job → Runs → Error logs**.

Re-run one day manually:
```bash
aws glue start-job-run --job-name transitpulse-silver \
  --arguments '{"--run_date":"2026-08-15"}' --region $REGION
```

An explicit `YYYY-MM-DD` processes exactly that day. The scheduler passes a full ISO
timestamp instead, which `resolve_run_date()` shifts back to the previous day.

---

## Weekly

- **Saturday:** confirm the static schedule refreshed
  ```bash
  aws logs tail /aws/lambda/transitpulse-static-loader --since 24h | tail -5
  ```
  A GTFS schedule change is a normal event, not a bug — but it is worth noting if
  something breaks that weekend.
- **Cost Explorer**, filtered to `Project = transitpulse`. Expect ~$5/week.
- **Confirm no SageMaker endpoint is running** that you forgot about:
  ```bash
  aws sagemaker list-endpoints
  ```

---

## Stepping away for more than a few days

```bash
make pause      # stops ingestion; everything else stays
```

The Kinesis shard still bills ~$0.36/day. Only `terraform destroy` stops that.

```bash
make resume     # start collecting again
```

---

## When to come back properly

**Around 1 September 2026** — Phase 5 needs at least 21 days of data for a
time-based split:

```
train:  service_date <= T-14d
val:    T-14d < service_date <= T-7d
test:   service_date >  T-7d
```

With fewer than ~21 days the training window is empty.

Before then, three things are known to be outstanding:

1. `transitpulse.training_features` has **no DDL anywhere** — gold writes plain
   parquet and never registers a table. `sql/04_baselines.sql` needs it.
2. Weather is not loaded, so 4 features fall back to constants.
3. `is_holiday` and `active_alert_on_route` are hardcoded to 0.
