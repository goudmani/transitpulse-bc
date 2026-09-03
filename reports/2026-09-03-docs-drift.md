# README drift check — 2026-09-03

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected is 23 of 21, not 2 collected.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines count is 0, so no pipeline is provisioned.

---

Facts at 2026-09-03 18:49 UTC:

```
- Service days collected: 23 of 21
- Stop arrivals in silver: 13,212,142
- Label completeness: 0.990
- MAE, published schedule: 154.9s
- MAE, persistence: 135.0s
- Persistence beats schedule by: 12.8%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-09-02: $0.81
- 3-day median usage: $0.89/day
```
