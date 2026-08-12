# ADR 002 — Apache Iceberg for the silver layer

## Status
Accepted

## Context
Stop events are corrected after the fact: a bus seen at 14:05 may produce a
better observation at 14:20, and a re-run of an earlier partition must not
duplicate rows. Plain partitioned Parquet forces a full-partition rewrite for
any correction.

## Decision
Store `stop_events` as an Iceberg table and use `MERGE INTO` keyed on
(service_date, trip_id, stop_id).

## Consequences
- Late-arriving corrections update in place; re-running a day is idempotent.
- Snapshot ids make training sets reproducible — a model can be traced to the
  exact table state it was trained on.
- Glue jobs need the `--datalake-formats iceberg` argument and a longer `--conf`
  string, which is a real source of setup friction.
- Small files accumulate; a periodic compaction job will be needed if the table
  grows past a few months of data.
