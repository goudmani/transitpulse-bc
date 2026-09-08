# TransitPulse BC

Real-time transit delay prediction on AWS: streaming GTFS-Realtime ingestion, an
S3 lakehouse, and an XGBoost delay model, all provisioned by Terraform, deployed
through GitHub Actions, and watched nightly by a read-only LLM agent that files
its own operations report.

> Transit data from TransLink, used under their Open API Terms of Use.
> Weather data from Open-Meteo.

## Status

<!-- agent:status:begin -->
**As of 2026-09-08, collection is complete and a model is registered.**

| Stage | State |
|---|---|
| Ingestion, bronze, silver, gold | complete — 27 service days, then stopped |
| Nightly ETL | stopped; nothing left to process |
| Nightly ops agent | running |
| Training, evaluation, model registry | **run; model registered, PendingManualApproval** |
| Inference endpoint, prediction API | infrastructure live, not deployed — see Known limitations |

Collection ran 2026-08-11 to 2026-09-06 and produced **15,525,290 gold rows** across 27 service days. The training pipeline trained, evaluated against three baselines, passed the registry gate at `mae_ratio_vs_best_baseline = 0.8975`, and registered the model.

Gross usage on 2026-09-06 was **$0.79/day**. The whole training pipeline cost roughly **$0.15** as a one-off.
<!-- agent:status:end -->

Collection was deliberately paced rather than rushed: 27 days, because a
time-based split needs enough distinct service days that the test week is a real
week and not an artefact.

> Figures in this README between `agent:*:begin` and `agent:*:end` markers are
> regenerated daily from live queries by the docs agent
> ([`docs/agent.md`](docs/agent.md)). Edit the queries in
> [`sql/07_profile_queries.sql`](sql/07_profile_queries.sql), not the numbers.

## Results

### Baselines, measured

<!-- agent:baselines:begin -->
Over 15,525,290 labelled stop arrivals, 2026-08-11 to 2026-09-06. Computed over the full 27-day training window. Baselines come from `sql/07_profile_queries.sql`; the model figure comes from the pipeline's own evaluation step.

| Predictor | MAE (seconds) |
|---|---|
| Published schedule (predict zero delay) | **152.1** |
| Persistence (bus stays as late as it currently is) | **130.2** |
| Historical median for route/stop/day-type/hour | **127.3** |
| **XGBoost model** | **114.3** |

The **XGBoost model reaches 114.3s** on 4,163,041 held-out arrivals — 10.2% better than the strongest baseline (historical, 127.3s) and 24.9% better than the printed timetable. The registry gate is `mae_ratio_vs_best_baseline <= 0.92`; it scored **0.8975** and was registered.
<!-- agent:baselines:end -->

| | |
|---|---|
| RMSE | 213.6s |
| Median absolute error | **72.1s** |
| 90th-percentile absolute error | 238.3s |
| Predictions within 60s of actual | **43.4%** |
| Predictions within 120s of actual | **69.6%** |

MAE is the headline because it is what the model optimises, but the median is
the number a rider would recognise: **half of all predictions land within 72
seconds of the true arrival delay.** The gap between the median (72s) and the
mean (114s) is the fat tail — a minority of buses whose lateness has no signal
in a schedule feed.

![Model versus baselines by hour of day](img/model_vs_baselines.png)

The aggregate hides the real finding. **The model's edge is not uniform — it is
concentrated in the hours that matter.** Between 09:00 and 19:00 it beats the
best baseline by 7–16%, peaking at 16% through the afternoon. Outside those
hours it is level with, or slightly worse than, simply looking up the historical
median for that stop.

That is the honest shape of the result: overnight, when a route is running four
buses and traffic is empty, a lookup table is as good as gradient boosting. The
model earns its keep in congestion, which is exactly when a rider cares.

![Feature importance: delay_t_minus_15 dominates](img/feature_importance.png)

**One feature carries 44% of the total split gain: `delay_t_minus_15`** — how
late the bus already was fifteen minutes before it arrived. That is worth sitting
with. The model's single strongest signal is the same information the persistence
baseline uses, and its advantage comes from knowing *when to trust it*: how far
into the route the bus is (`stop_sequence`, `shape_dist_traveled` — together 16%),
what this stop normally looks like at this hour (`hist_median_delay`,
`hist_p90_delay` — 10%), and what the buses ahead of it are doing
(`prev_stop_delay`, `upstream_delay_same_trip`, `preceding_trip_delay` — 9%).

The four weather features contribute almost nothing. Collected over 27 late-summer
days in Vancouver, there was barely any weather to learn from.

"A bus four minutes late tends to stay four minutes late" is a hard baseline, and
a model that only ties it is a real finding rather than a failure to hide. A gate
that always passes is decoration.

The gate is wired to whichever baseline is strongest, which turned out **not** to
be persistence. On the held-out test week the historical median reaches 127.3s
against persistence's 130.2s. Restricting both to the 3,489,305 rows that actually
have a prior — so neither gets credit for the fallback — the gap widens: **126.1s
versus 131.5s**. Delay at a given stop is more a property of that stop at that
hour than of the individual bus. Had the gate stayed on persistence, a model at
125s would have registered while a lookup table beat it.

The historical-median baseline needs `hist_median_delay`, which requires ≥20
observations per route/stop/day-type/hour cell from *strictly earlier* service
dates. That is the leakage guard, and it means the feature is empty until roughly
day five, and null for 16% of test rows even now.

All measured on a time-based hold-out split, never random. A random split leaks
the future through the historical aggregates and makes every metric fraudulent.

### How the split and the model were built

| | rows | service dates |
|---|---|---|
| train | 7,233,823 | 2026-08-11 → 08-23 |
| validation | 4,128,426 | 2026-08-24 → 08-30 |
| test | 4,163,041 | 2026-08-31 → 09-06 |

Two contiguous weeks held out, never shuffled. The split key is `service_date`,
the GTFS service day — so a trip that departs at 23:40 and arrives at 00:20 falls
entirely on one side of the boundary rather than straddling it.

The model is XGBoost with `objective: reg:absoluteerror` — the loss it is scored
on, not a proxy for it. 26 features, `max_depth 8`, `eta 0.08`, up to 800 boosting
rounds with early stopping at 50.

**It stopped at round 64, with the best validation score at round 15.** Fifteen
trees. After that, training error kept falling while validation error rose: the
model began learning the specific fortnight it was shown rather than how buses
behave. With 7.2M training rows that is not a shortage of data — it is temporal
distribution shift, two different weeks of a city. Early stopping is what turned
that from a silent overfit into a fifteen-tree model that generalises.

## Architecture

EventBridge → Lambda poller → Kinesis Data Streams → Firehose → S3 bronze → Glue
PySpark (bronze → silver Iceberg → gold) → SageMaker Pipeline (train → evaluate →
quality gate → registry) → Serverless Inference endpoint → Lambda → API Gateway.

Everything up to and including gold runs nightly today. Everything from the
SageMaker Pipeline onward is provisioned and waiting for a training window.

Two branches of that diagram are switched off on purpose:

- **The DynamoDB online-feature path** (Kinesis → Lambda → DynamoDB) has its event
  source mapping disabled. Nothing reads it during collection, because training
  data comes entirely from S3, and it was 49% of the bill. It has to be re-enabled
  before an endpoint is deployed.
- **The prediction API** is live at the infrastructure level, Lambda and API
  Gateway both, but there is no inference endpoint behind it yet.

### ETL orchestration

![Step Functions state machine: silver, data quality gate, gold](img/stepfunctions_graph.png)

The nightly ETL is a Step Functions state machine, not a chain of cron jobs. The
detail worth noting is that **data quality failure and infrastructure failure
take different branches**:

- `DataQualityChecks` exits non-zero → `QuarantinePartition` → the gold layer is
  never rebuilt, the bad partition stays inspectable in S3, and an SNS alert
  fires. Bad data cannot reach training.
- Any other task failing → `NotifyFailure` → `FailPipeline`.

Both are failures, but they are different problems and deserve different
responses. Treating them identically would either crash the pipeline on
recoverable data issues or silently promote bad data on a retry.

Decisions and their trade-offs are recorded in `docs/adr/`.

## What the data looks like

<!-- agent:dataprofile:begin -->
15,849,990 stop arrivals over 28 days of collection (2026-08-11 to 2026-09-07), label completeness 0.990.
<!-- agent:dataprofile:end -->

The charts below are drawn by `python scripts/plot_profile.py` from the CSVs in
`data/processed/`, which `scripts/build_demo_data.py` regenerates from the same
held-out split the model was scored on. One source of numbers for the charts, the
metrics and the demo page.

![Arrival delay distribution: 42% late, 38% on time, 21% early](img/delay_distribution.png)

Buses run late more often than early, but the distribution is wide in both
directions: 21% of arrivals are more than a minute *ahead* of schedule. That is
why the model predicts signed delay rather than lateness, and why `clamp_delay()`
has a floor of −1800 seconds rather than zero.

![Hourly service volume and delay profile](img/hourly_profile.png)

The 46× swing in arrivals between 3am and the afternoon peak is the feed
reflecting how many buses are actually on the road. The second panel is the
interesting one: **volume and delay do not move together.** The busiest hours are
not the worst ones, and the quietest hour of the night carries a higher mean delay
than the morning rush.

Volume and delay are plotted on separate stacked axes rather than a shared one,
because a dual-axis chart lets you imply any correlation you like by sliding the
scales.

> **This chart found a bug — and this is the fixed version.** `hour_of_day` was
> derived from `observed_arrival_ts`, which is UTC, while `PEAK_HOURS = {7, 8, 15,
> 16, 17}` was written for local hours: midnight, 1am and mid-morning in Vancouver.
> Every arrival after 17:00 local — roughly 29% of rows, and the second-busiest
> stretch of the day — was stamped with the following day's date, so `day_of_week`
> and `is_weekend` flipped too. Meanwhile the serving path computed its hour from
> `now + LOCAL_OFFSET` before calling the same `is_peak_hour()`. Training read UTC,
> serving read local: seven hours apart for the same bus.
>
> `gold_features.py` now re-derives all three features with
> `from_utc_timestamp(..., "America/Vancouver")` — a real zone, not a fixed −7,
> because the collection window crosses the PDT/PST boundary. Gold was rebuilt for
> all 27 days before the split was cut, so the model above never saw the bad
> values. Written up as [ADR 005](docs/adr/005-timezone-boundaries.md), which
> treats four separate defects as one pattern: **a timestamp is not a time until
> you say where it is.**
>
> It was caught by plotting the data, not by a test. The earlier version of this
> chart showed a bus network with no overnight trough, which is not a thing that
> exists.

## Repository layout

| Path | Contents |
|---|---|
| `infra/` | Terraform: 8 modules, root composition, dev/prod tfvars |
| `src/common/features.py` | The feature contract shared by training and serving |
| `src/ingest/` | Poller (zip), static GTFS loader, online feature writer |
| `src/glue/` | PySpark ETL: silver, gold, data quality, backtest |
| `src/ml/` | SageMaker training, evaluation, pipeline, deploy |
| `src/serving/predict/` | Prediction Lambda behind API Gateway |
| `sql/` | Athena DDL, baseline query, phase verification queries |
| `tests/` | Unit tests plus the training/serving parity guard |
| `scripts/` | Bootstrap, image build, backfill, daily health check |
| `agent/` | Ops agent: supervisor, four subagents, 18 read-only tools |
| `reports/` | One agent report per day, committed by the nightly workflow |

## Quick start

```bash
make lint test          # runs offline, no AWS needed
./scripts/bootstrap.sh  # one-time state backend
make package            # build the poller as an 868 KB zip, no Docker
make init plan apply
```

The poller ships as a zip rather than a container. `pip --platform
manylinux2014_x86_64` cross-builds Linux wheels from macOS, so the same command
produces an identical package on Apple Silicon and Intel: 868 KB instead of a
~600 MB image, and no Docker daemon. `make image` still exists if you set
`poller_package_type = "Image"`.

Day-to-day operation is in [`docs/daily-runbook.md`](docs/daily-runbook.md):
`make check` for service health, `make data` for collection progress. Both are
also run nightly, unattended, by [the ops agent](#the-ops-agent).

## Production considerations

- **Data quality gate.** Bad partitions are quarantined and the gold layer is
  left untouched, rather than crashing the pipeline or poisoning training data.
- **Point-in-time correctness.** Historical aggregates use strictly earlier
  service dates; the split is time-based, never random.
- **Training/serving parity.** One feature module, one set of null-fill
  defaults, and a test that fails when the two paths diverge.
- **Late-arriving ground truth.** A daily backtest job joins captured predictions
  to observed outcomes and publishes rolling MAE to CloudWatch. Written and
  deployed, but not yet exercised: there are no predictions to backtest until an
  endpoint exists.
- **Cost control.** No NAT Gateway, serverless inference, S3 lifecycle rules,
  explicit log retention, and a billing alarm that disables ingestion.

## Cost

**~$21/month at current ingest volume**, measured rather than estimated. It did
not start there. The first days ran at ~$4/day, and getting it down meant reading
an itemised bill rather than trusting an architecture diagram.

| Change | Why |
|---|---|
| Disabled the online-feature path until serving | Nothing reads DynamoDB during collection; training data comes entirely from S3. It was 49% of spend. |
| Pack ~100 rows per Kinesis record | Firehose bills every record rounded up to **5 KB**, and rows are ~200 bytes, so unpacked it billed 57 GB/day against 2.3 GB of real data. |
| Poll every 2 min instead of 1 | Silver emits one row per *arrival* regardless of how often it was re-predicted, so this halves cost for the same number of training rows. |
| Daily ETL instead of hourly | 72 → 3 Glue runs/day, for data a weekly retrain consumes. |
| No NAT Gateway | Two free gateway VPC endpoints replace a ~$32/month appliance. See [`adr/003`](docs/adr/003-no-nat-gateway.md). |
| Serverless inference chosen over a provisioned endpoint | ~$0 idle versus ~$96/month for the smallest always-on endpoint. See [`adr/004`](docs/adr/004-serverless-inference.md). |

The Kinesis shard is the only component that bills while idle, at ~$0.36/day.
Everything else genuinely scales to zero, which is why the current bill is almost
entirely that shard.

## The ops agent

A supervisor and four specialists run at 03:30 UTC, inspect the running pipeline
and commit a report to [`reports/`](reports/). LangChain on Groq, traced in
LangSmith, deployed as a scheduled GitHub Actions workflow, so it runs whether or
not a laptop is open.

| Agent | The question it answers |
|---|---|
| infrastructure | Is it running? Alarms, EventBridge rules, Kinesis/Firehose, Lambda + DLQ, Step Functions, Glue |
| cost | Is it spending more than it should, and on what? |
| data quality | Is the output usable for training? Collection progress, label rate, duplicates |
| code | Does any of that trace to a defect in this repo? |
| supervisor | Compiles the four and writes the report |

The decision worth defending is that **the subagents do not compute anything.**
Every number in a report came from a boto3 or Athena call, threshold comparisons
and the month-end projection happen in Python, and the supervisor carries findings
through verbatim. The only free prose is the two-sentence summary at the top. A
model having an off day makes that summary bland; it cannot invent a cost figure
or silently drop a critical finding.

It is read-only by construction: a dedicated IAM role with an explicit `Deny` on
every mutating action, an Athena tool that rejects anything but `SELECT`, and repo
tools that cannot escape the working tree. That matters more than it first appears,
because the agent reads CloudWatch logs, which carry data derived from an external
feed and are therefore a real prompt-injection channel. Injection cannot be
reliably prevented by prompting, so the blast radius is constrained instead: the
code agent proposes patches as a **draft pull request** and is structurally unable
to apply one.

Each run is a single LangSmith trace: four subagents, every model call and every
tool call nested under one root span, stamped with the git SHA and the thresholds
in force. The report says what the agent concluded; the trace is how you check its
working, and its URL is in the report footer.

```bash
make agent-tools   # exercise every tool against AWS, no model, no tokens spent
make agent         # full run, writes reports/<today>.md
```

Running it costs effectively nothing: Groq free tier, ~5 minutes a day of Actions
minutes, and metric reads. That was a design constraint rather than a happy
result, because an observability layer costing more than the ~$0.70/day pipeline
it watches is a bad trade. Details in [`docs/agent.md`](docs/agent.md).

## Known limitations

- **No endpoint is deployed.** The model is trained, evaluated and registered, but
  nothing serves it, and that is a decision rather than an omission. The online
  feature store is empty: the DynamoDB writer was disabled in August to cut cost,
  so an endpoint would fall back to `DEFAULTS` for the four `hist_*` features —
  15% of the model's total gain — and quietly return predictions worse than the
  114.3s reported here. A demo that serves degraded predictions is worse than no
  demo. See [the static demo](https://goudmani.github.io/transit-pulse-bc/), which
  scores real held-out rows with the real artifact.
- **Known training/serving skew, unresolved.** `src/serving/predict/features.py`
  uses a fixed `LOCAL_OFFSET = timedelta(hours=-7)` while training is DST-aware via
  `from_utc_timestamp(..., "America/Vancouver")`. Correct until 2 November, wrong
  by an hour after it. `tests/test_feature_parity.py` fails on this by design —
  it is the test doing its job, not a broken test.
- **The model is fifteen trees.** Early stopping halted at round 64 with the best
  iteration at 15, and `booster.predict()` uses all 64 by default rather than the
  best 15, so roughly a second of MAE is left on the table. Reported as measured.
- **27 days is one season.** Collected 11 August to 6 September: no snow, no ice,
  no winter darkness, one long weekend. The four weather features contribute
  almost nothing to the model because there was almost no weather. A model trained
  here would need retraining before it could be trusted in January.
- The SageMaker execution role uses `AmazonSageMakerFullAccess` for development.
  A production deployment would scope this down.
- One Kinesis shard caps writes at 1,000 records/sec. At ~16,000 rows per poll the
  poller exceeded that, Kinesis throttled, invocations failed and polls were lost
  to the DLQ. Resolved by pacing `PutRecords` batches to ~900/sec inside the Lambda
  rather than provisioning a second shard: the function has a 120-second timeout
  and was finishing in 4.5, so the capacity was already paid for. A second agency
  would need a real second shard.
- Iceberg small files will need periodic compaction beyond a few months of data.
- The ops agent has no memory between runs, so a slow drift that never breaches a
  daily threshold is invisible to it. Worse, nothing alerts when the *workflow
  itself* fails to fire. Absence is silent, which is precisely the class of bug
  the agent was written to catch in the ETL. A dead-man's-switch is the fix.
