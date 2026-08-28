# README drift check — 2026-08-28

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines count is 0 (no pipelines deployed)

- **certain** — "because a time-based split needs 21 service days and 2 are collected."
  Service days collected is 16 of 21 (not 2)

- **certain** — "The Kinesis shard is the only component that bills while idle, at ~$0.36/day."
  Gross usage on 2026-08-27 is $0.00 (no cost incurred)

---

Facts at 2026-08-28 00:29 UTC:

```
- Service days collected: 16 of 21
- Stop arrivals in silver: 9,031,509
- Label completeness: 0.990
- MAE, published schedule: 153.3s
- MAE, persistence: 135.0s
- Persistence beats schedule by: 11.9%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-27: $0.00
- 3-day median usage: $1.01/day
```
