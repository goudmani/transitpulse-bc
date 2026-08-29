# README drift check — 2026-08-29

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines: 0

- **certain** — "because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 17 of 21

---

Facts at 2026-08-29 00:03 UTC:

```
- Service days collected: 17 of 21
- Stop arrivals in silver: 9,674,362
- Label completeness: 0.990
- MAE, published schedule: 152.9s
- MAE, persistence: 134.7s
- Persistence beats schedule by: 11.9%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-28: $0.68
- 3-day median usage: $1.05/day
```
