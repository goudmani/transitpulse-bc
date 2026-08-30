# README drift check — 2026-08-30

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 19 of 21 (the README says only 2 are collected)

---

Facts at 2026-08-30 18:41 UTC:

```
- Service days collected: 19 of 21
- Stop arrivals in silver: 10,845,509
- Label completeness: 0.989
- MAE, published schedule: 155.6s
- MAE, persistence: 136.0s
- Persistence beats schedule by: 12.6%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-29: $1.02
- 3-day median usage: $1.05/day
```
