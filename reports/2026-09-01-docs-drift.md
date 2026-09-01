# README drift check — 2026-09-01

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines: 0

- **certain** — "**~$21/month at current ingest volume**, measured rather than estimated."
  Gross usage on 2026-08-31: $1.01 (≈ $30/month)

---

Facts at 2026-09-01 18:41 UTC:

```
- Service days collected: 21 of 21
- Stop arrivals in silver: 11,922,440
- Label completeness: 0.990
- MAE, published schedule: 156.2s
- MAE, persistence: 136.1s
- Persistence beats schedule by: 12.9%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-31: $1.01
- 3-day median usage: $1.01/day
```
