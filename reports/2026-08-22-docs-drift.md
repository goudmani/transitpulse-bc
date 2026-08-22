# README drift check — 2026-08-22

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines: 0 (no pipelines deployed)

- **certain** — "The Kinesis shard is the only component that bills while idle, at ~$0.36/day."
  Gross usage on 2026-08-21: $0.86 (actual daily cost)

- **certain** — "Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 11 of 21 (actual collected days)

---

Facts at 2026-08-22 15:44 UTC:

```
- Service days collected: 11 of 21
- Stop arrivals in silver: 6,131,423
- Label completeness: 0.989
- MAE, published schedule: 157.2s
- MAE, persistence: 137.9s
- Persistence beats schedule by: 12.3%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-21: $0.86
- 3-day median usage: $0.86/day
```
