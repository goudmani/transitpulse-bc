# TransitPulse BC

Real-time transit delay prediction on AWS: streaming GTFS-Realtime ingestion,
an S3 lakehouse, and a deployed SageMaker model that beats the published
schedule — all provisioned by Terraform and deployed through GitHub Actions.

> Transit data from TransLink, used under their Open API Terms of Use.
> Weather data from Open-Meteo.

## Results

### Baselines — measured

Over 775,092 stop arrivals, 2026-08-11 to 08-12. **Preliminary**: two days is one
weekday and one partial day, with no weekend, no rain and no incident. Final
figures come from `sql/04_baselines.sql` over the full training window.

| Predictor | MAE (seconds) |
|---|---|
| Published schedule (predict zero delay) | **156.2** |
| Persistence (bus stays as late as it currently is) | **136.4** |
| Historical median for route/stop/hour | pending — needs ≥5 days of history |
| **XGBoost model** | pending — Phase 6 |

Persistence beats the printed timetable by 12.7%, which is the honest bar. "A bus
four minutes late tends to stay four minutes late" is a hard baseline, and a model
that only ties it is a real finding rather than a failure to hide.

The registry gate is set at `mae_ratio_vs_persistence <= 0.92`, so a model must
reach **≤ 125.5 seconds** to be registered at all. A gate that always passes is
decoration.

The historical-median baseline needs `hist_median_delay`, which requires ≥20
observations per route/stop/day-type/hour cell from *strictly earlier* service
dates. That is the leakage guard, and it means the feature is empty until roughly
day five.

All measured on a time-based hold-out split — never random. A random split leaks
the future through the historical aggregates and makes every metric fraudulent.

## Architecture

EventBridge → Lambda poller → Kinesis Data Streams → { Firehose → S3 bronze,
Lambda → DynamoDB online features } → Glue PySpark (bronze → silver Iceberg →
gold) → SageMaker Pipeline (train → evaluate → quality gate → registry) →
Serverless Inference endpoint → Lambda → API Gateway.

### ETL orchestration

![Step Functions state machine: silver, data quality gate, gold](img/stepfunctions_graph.png)

The nightly ETL is a Step Functions state machine, not a chain of cron jobs. The
detail worth noting is that **data quality failure and infrastructure failure
take different branches**:

- `DataQualityChecks` exits non-zero → `QuarantinePartition` → the gold layer is
  never rebuilt, the bad partition stays inspectable in S3, and an SNS alert
  fires. Bad data cannot reach the model.
- Any other task failing → `NotifyFailure` → `FailPipeline`.

Both are failures, but they are different problems and deserve different
responses. Treating them identically would either crash the pipeline on
recoverable data issues or silently promote bad data on a retry.

Decisions and their trade-offs are recorded in `docs/adr/`.

## What the data looks like

775,092 stop arrivals collapsed from 16.7 million raw predictions, over the first
two days of collection. Regenerate with `python scripts/plot_profile.py`.

![Arrival delay distribution: 40% late, 37% on time, 23% early](img/delay_distribution.png)

Buses run late more often than early, but the distribution is wide in both
directions — 23% of arrivals are more than a minute *ahead* of schedule. That is
why the model predicts signed delay rather than lateness, and why `clamp_delay()`
has a floor of −1800 seconds rather than zero.

![Hourly service volume and delay profile](img/hourly_profile.png)

The 46× swing in arrivals between 3am and the afternoon peak is the feed
reflecting how many buses are actually on the road. The second panel is the
interesting one: **volume and delay do not move together.** The busiest hours are
not the worst ones, and the quietest hour of the night carries a higher mean delay
than the morning rush.

Volume and delay are plotted on separate stacked axes rather than a shared one —
a dual-axis chart lets you imply any correlation you like by sliding the scales.

> **This chart found a bug.** `hour_of_day` is derived from `observed_arrival_ts`,
> which is UTC, but `PEAK_HOURS = {7, 8, 15, 16, 17}` was written for local hours —
> midnight, 1am, and mid-morning in Vancouver. Meanwhile the serving path computes
> its hour from `now + LOCAL_OFFSET` before calling the same `is_peak_hour()`.
> Training reads UTC, serving reads local: seven hours apart for the same bus.
> Textbook training/serving skew, caught by plotting the data rather than by a test.

## Repository layout

| Path | Contents |
|---|---|
| `infra/` | Terraform: 7 modules, root composition, dev/prod tfvars |
| `src/common/features.py` | The feature contract shared by training and serving |
| `src/ingest/` | Poller (container), static GTFS loader, online feature writer |
| `src/glue/` | PySpark ETL: silver, gold, data quality, backtest |
| `src/ml/` | SageMaker training, evaluation, pipeline, deploy |
| `src/serving/predict/` | Prediction Lambda behind API Gateway |
| `sql/` | Athena DDL, baseline query, phase verification queries |
| `tests/` | Unit tests plus the training/serving parity guard |
| `scripts/` | Bootstrap, image build, backfill, daily health check |

## Quick start

```bash
make lint test          # runs offline, no AWS needed
./scripts/bootstrap.sh  # one-time state backend
make package            # build the poller as an 868 KB zip -- no Docker
make init plan apply
```

The poller ships as a zip rather than a container. `pip --platform
manylinux2014_x86_64` cross-builds Linux wheels from macOS, so the same command
produces an identical package on Apple Silicon and Intel — 868 KB instead of a
~600 MB image, and no Docker daemon. `make image` still exists if you set
`poller_package_type = "Image"`.

Day-to-day operation is in [`docs/daily-runbook.md`](docs/daily-runbook.md):
`make check` for service health, `make data` for collection progress.

## Production considerations

- **Data quality gate** — bad partitions are quarantined and the gold layer is
  left untouched, rather than crashing the pipeline or poisoning training data.
- **Point-in-time correctness** — historical aggregates use strictly earlier
  service dates; the split is time-based, never random.
- **Training/serving parity** — one feature module, one set of null-fill
  defaults, and a test that fails when the two paths diverge.
- **Late-arriving ground truth** — a daily backtest joins captured predictions
  to observed outcomes and publishes rolling MAE to CloudWatch.
- **Cost control** — no NAT Gateway, serverless inference, S3 lifecycle rules,
  explicit log retention, and a billing alarm that disables ingestion.

## Cost

**~$21/month at ~11.5M events/day**, measured rather than estimated. It did not
start there — the first days ran at ~$4/day, and getting it down meant reading an
itemised bill rather than trusting an architecture diagram.

| Change | Why |
|---|---|
| Disabled the online-feature path until serving | Nothing reads DynamoDB during collection; training data comes entirely from S3. It was 49% of spend. |
| Pack ~100 rows per Kinesis record | Firehose bills every record rounded up to **5 KB**, and rows are ~200 bytes — unpacked it billed 57 GB/day against 2.3 GB of real data. |
| Poll every 2 min instead of 1 | Silver emits one row per *arrival* regardless of how often it was re-predicted, so this halves cost for the same number of training rows. |
| Daily ETL instead of hourly | 72 → 3 Glue runs/day, for data a weekly retrain consumes. |
| No NAT Gateway | Two free gateway VPC endpoints replace a ~$32/month appliance — [`adr/003`](docs/adr/003-no-nat-gateway.md). |
| Serverless inference | ~$0 idle versus ~$96/month for the smallest always-on endpoint — [`adr/004`](docs/adr/004-serverless-inference.md). |

The Kinesis shard is the only component that bills while idle, at ~$0.36/day.
Everything else genuinely scales to zero.

## Known limitations

- The SageMaker execution role uses `AmazonSageMakerFullAccess` for
  development. A production deployment would scope this down.
- One Kinesis shard caps writes at 1,000 records/sec. At ~16,000 rows per poll
  the poller exceeded that, Kinesis throttled, invocations failed and polls were
  lost to the DLQ. Resolved by pacing `PutRecords` batches to ~900/sec inside the
  Lambda rather than provisioning a second shard — the function has a 120-second
  timeout and was finishing in 4.5, so the capacity was already paid for. A second
  agency would need a real second shard.
- Iceberg small files will need periodic compaction beyond a few months of data.

