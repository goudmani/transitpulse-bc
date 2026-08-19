# README drift check — 2026-08-18

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines deployed: 0 (no pipelines exist)

- **certain** — "because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 7 of 21 (7 days have been collected, not 2)

---

Facts at 2026-08-18 21:42 UTC:

```
- Service days collected: 7 of 21
- Stop arrivals in silver: 3,554,281
- Label completeness: 0.987
- MAE, published schedule: 166.2s
- MAE, persistence: 144.5s
- Persistence beats schedule by: 13.1%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-08-17: $0.84
- 3-day median usage: $0.84/day
```
