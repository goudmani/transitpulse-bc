# ADR 005 — Timezones at system boundaries

## Status
Accepted. One consequence is still open — see below.

## Context

Four separate defects in this project trace to the same root cause: a timestamp
crossing a boundary between two systems without its timezone travelling with it.
Vancouver is UTC−7 in summer and UTC−8 in winter, and every one of these failures
was silent — no exception, no null, no failed job.

**1. Temporal features derived in UTC, thresholds written in local.**
`silver_stop_events.py` built `hour_of_day`, `day_of_week` and `is_weekend` with
`F.hour(observed_arrival_ts)` where `observed_arrival_ts` is UTC. But
`PEAK_HOURS = {7, 8, 15, 16, 17}` in `gold_features.py` was written for local
hours. In UTC those are 00:00, 01:00, 08:00, 09:00 and 10:00 Vancouver time, so
`is_peak` flagged the middle of the night and missed the afternoon rush entirely.

Worse, arrivals after 17:00 local fall on the *following* UTC day, so a Friday
20:00 bus was labelled `day_of_week = Saturday` with `is_weekend = 1`. Measured
against the hourly profile, **~29% of rows** sit in that window — the
second-busiest stretch of the service day. Three of 26 features were wrong on
nearly a third of the training set.

**2. The weather join, nearly.** `gold_features.py` joins weather on
`today.hour_of_day == weather.weather_hour`. Both sides are integers 0–23, so
requesting the Open-Meteo archive in UTC while `hour_of_day` was local would have
joined cleanly and been **seven hours out of alignment on every row** — each
arrival carrying the weather from the middle of the previous night. No error, no
null, nothing to notice except wondering why rain had no predictive power. Caught
before it shipped, not after.

**3. Bronze partitions are UTC days; `service_date` is local.** Silver reads one
bronze partition per run, and `dt=2026-09-06` spans 17:00 Sep 5 through 17:00
Sep 6 local. Every arrival after 17:00 on the final collected day lands in the
*next* partition, which no run ever processes. The last day of any collection
window is therefore short by seven hours of evening service — 285k rows against
~450k for a comparable day — and it is precisely the day that ends up in a
time-based test split.

**4. A fixed offset in the serving path.**
`src/serving/predict/handler.py` uses `LOCAL_OFFSET = timedelta(hours=-7)`,
correct during PDT and wrong from the first Sunday in November. The comment
acknowledges it; the code still ships it.

## Decision

**Store instants in UTC. Derive anything a human or a threshold reasons about in
the display timezone. Never compare an hour integer from one system against an
hour integer from another without establishing which zone each came from.**

Concretely:

- `observed_arrival_ts` stays UTC. It is an instant, and instants have no zone.
- `gold_features.py` re-derives `hour_of_day`, `day_of_week` and `is_weekend` in
  `read_silver()` using `from_utc_timestamp(..., "America/Vancouver")`, which
  resolves DST from the zone database rather than assuming a fixed offset. The
  re-derivation is applied to the *whole* table, not just the run date, because
  `historical_priors()` groups by `is_weekend` and `hour_of_day` and joins back
  on both — correcting one side only would have silently missed the join.
- `scripts/load_weather.py` requests `timezone=America/Vancouver`, so the join
  key is local on both sides by construction.
- Recovering a collection window means processing **one bronze partition past
  the end of it**.

Fixed in gold rather than silver deliberately: silver reads bronze, bronze
expires at 30 days, and the earliest collected days were already near that edge.
`observed_arrival_ts` is a correct UTC instant, so gold can re-derive without
touching bronze at all.

## Consequences

- 27 days of gold rebuilt. Silver keeps its UTC-derived columns; **gold overrides
  them, so anything hour-related must be queried from `training_features`, never
  from `stop_events`.** That is a trap left in place to avoid a bronze-dependent
  rebuild, and it is the main cost of this decision.
- `is_peak` now matches the two rushes visible in the hourly profile.
- Weather joins on local hours on both sides.
- **Open:** the serving path still uses a fixed `LOCAL_OFFSET`. Training now uses
  a DST-aware conversion, so from November the two will disagree by an hour for
  the same bus. `tests/test_feature_parity.py` exists to catch exactly this and
  is expected to fail on it. Fixing it properly means bundling `tzdata` into the
  Lambda zip or storing a UTC-based hour in the online feature store.

## Notes

None of these four would have been caught by a test asserting the code runs. The
checks that caught them were an hourly count that peaked at the wrong time, a row
count 40% below its neighbours, and reading a join condition carefully before
trusting it. Verify the output, not the exit status.
