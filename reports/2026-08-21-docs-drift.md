# README drift check — 2026-08-21

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Training, evaluation, the registry gate and the endpoint are written and provisioned but have never run, because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 10 of 21

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines: 0

---

Facts at 2026-08-21 15:57 UTC:

```
- Service days collected: 10 of 21
- Stop arrivals in silver: 5,488,153
- Label completeness: 0.989
- MAE, published schedule: 157.5s
- MAE, persistence: 138.3s
- Persistence beats schedule by: 12.2%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-20: $0.86
- 3-day median usage: $0.91/day
```
