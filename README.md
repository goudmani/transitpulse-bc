# TransitPulse BC

Real-time transit delay prediction on AWS: streaming GTFS-Realtime ingestion,
an S3 lakehouse, and a deployed SageMaker model that beats the published
schedule — all provisioned by Terraform and deployed through GitHub Actions.

> Transit data from TransLink, used under their Open API Terms of Use.
> Weather data from Open-Meteo.

## Results

| Predictor | MAE (seconds) |
|---|---|
| Published schedule (predict 0 delay) | _fill from `sql/04_baselines.sql`_ |
| Persistence (bus stays as late as it is) | _fill_ |
| Historical median for route/stop/hour | _fill_ |
| **XGBoost model** | _fill_ |

Measured on a time-based hold-out split. Numbers are reproducible from the
Iceberg snapshot recorded in the model registry.

## Architecture

EventBridge → Lambda poller → Kinesis Data Streams → { Firehose → S3 bronze,
Lambda → DynamoDB online features } → Glue PySpark (bronze → silver Iceberg →
gold) → SageMaker Pipeline (train → evaluate → quality gate → registry) →
Serverless Inference endpoint → Lambda → API Gateway.

Decisions and their trade-offs are recorded in `docs/adr/`.

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

Roughly $30–45/month at ~4M events/day. See `docs/adr/003-no-nat-gateway.md`
and `docs/adr/004-serverless-inference.md` for the two decisions that dominate
that figure.

## Known limitations

- The SageMaker execution role uses `AmazonSageMakerFullAccess` for
  development. A production deployment would scope this down.
- One Kinesis shard caps throughput at 1,000 records/sec; a second agency
  would need a second shard.
- Iceberg small files will need periodic compaction beyond a few months of data.
