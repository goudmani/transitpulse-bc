# TransitPulse BC

Real-time transit delay prediction on AWS: streaming GTFS-Realtime ingestion,
an S3 lakehouse, and a deployed SageMaker model that beats the published
schedule — all provisioned by Terraform and deployed through GitHub Actions.

> Transit data from TransLink, used under their Open API Terms of Use.
> Weather data from Open-Meteo.

## Results

| Predictor | MAE (seconds) |
|---|---|
| Published schedule (predict 0 delay) |  |
| Persistence (bus stays as late as it is) |  |
| Historical median for route/stop/hour |  |
| **XGBoost model** |  |

Measured on a time-based hold-out split. Numbers are reproducible from the
Iceberg snapshot recorded in the model registry.

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
make image              # build and push the poller container
make init plan apply
```

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

Roughly $30–45/month at ~16M events/day. See `docs/adr/003-no-nat-gateway.md`
and `docs/adr/004-serverless-inference.md` for the two decisions that dominate
that figure.

## Known limitations

- The SageMaker execution role uses `AmazonSageMakerFullAccess` for
  development. A production deployment would scope this down.
- One Kinesis shard caps throughput at 1,000 records/sec; a second agency
  would need a second shard.
- Iceberg small files will need periodic compaction beyond a few months of data.

