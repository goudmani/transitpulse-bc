# README drift check — 2026-09-08

Claims that appear to contradict live facts. **Not auto-corrected**: a sentence is an argument, and rewriting one should be a decision.

- **certain** — "because a time-based split needs 21 service days and 2 are collected."
  Service days collected: 28 of 21 (i.e., more than the required 21 days have been collected)

- **certain** — "On the held-out test week the historical median reaches 127.3s against persistence's 130.2s."
  MAE, historical median: 140.0s (live fact) vs 127.3s claimed

- **certain** — "Restricting both to the 3,489,305 rows that actually have a prior — so neither gets credit for the fallback — the gap widens: 126.1s versus 131.5s."
  MAE, persistence: 134.7s (live fact) vs 131.5s claimed; also historical median 140.0s vs 126.1s claimed

---

Facts at 2026-09-08 18:52 UTC:

```
- Service days collected: 28 of 21
- Stop arrivals in silver: 15,849,990
- Label completeness: 0.990
- MAE, published schedule: 155.3s
- MAE, persistence: 134.7s
- MAE, historical median: 140.0s
- Best baseline: persistence at 134.7s, beating schedule by 13.3%
- SageMaker endpoints deployed: 0
- Registered model packages: 0
- SageMaker pipelines: 0
- Ingestion EventBridge rule enabled: False
- Last ETL execution: SUCCEEDED
- Gross usage on 2026-09-07: $2.00
- 3-day median usage: $0.82/day
```
