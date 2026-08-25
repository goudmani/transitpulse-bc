# README drift check — 2026-08-25

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "- **No model exists yet.** Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 14 of 21 (the README claims only 2 days have been collected)

---

Facts at 2026-08-25 16:08 UTC:

```
- Service days collected: 14 of 21
- Stop arrivals in silver: 7,744,902
- Label completeness: 0.990
- MAE, published schedule: 153.9s
- MAE, persistence: 135.4s
- Persistence beats schedule by: 12.0%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-24: $0.89
- 3-day median usage: $0.81/day
```
