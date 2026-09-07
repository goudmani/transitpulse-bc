# README drift check — 2026-09-07

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 27 of 21 (not 2)

- **certain** — "Everything from the SageMaker Pipeline onward is provisioned and waiting for a training window."
  SageMaker pipelines deployed: 0 (none provisioned)

---

Facts at 2026-09-07 19:19 UTC:

```
- Service days collected: 27 of 21
- Stop arrivals in silver: 15,506,255
- Label completeness: 0.990
- MAE, published schedule: 155.1s
- MAE, persistence: 134.7s
- Persistence beats schedule by: 13.2%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: True
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-09-06: $0.79
- 3-day median usage: $0.82/day
```
