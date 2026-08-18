# README drift check — 2026-08-15

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Serverless inference chosen over a provisioned endpoint"
  SageMaker endpoints deployed: 0

- **certain** — "SageMaker training, evaluation, pipeline, deploy"
  Registered model packages: 0

- **certain** — "~$21/month at current ingest volume"
  Gross usage on 2026-08-14: $1.03

---

Facts at 2026-08-15 06:01 UTC:

```
- Arrivals late (>1 min): 41.8%
- Arrivals on time (within 1 min): 36.8%
- Arrivals early (>1 min ahead): 21.5%
- Service days collected: 4 of 21
- Stop arrivals in silver: 1,942,280
- Label completeness: 0.989
- MAE, published schedule: 188.8s
- MAE, persistence: 161.0s
- Persistence beats schedule by: 14.7%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-14: $1.03
- 3-day median usage: $1.03/day
```
