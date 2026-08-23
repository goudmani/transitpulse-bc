# README drift check — 2026-08-23

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines count is 0, but the README says a pipeline is provisioned.

- **certain** — "Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected are 12 of 21, not 2 as stated.

---

Facts at 2026-08-23 15:45 UTC:

```
- Service days collected: 12 of 21
- Stop arrivals in silver: 6,666,379
- Label completeness: 0.990
- MAE, published schedule: 156.1s
- MAE, persistence: 137.1s
- Persistence beats schedule by: 12.2%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-22: $0.81
- 3-day median usage: $0.86/day
```
