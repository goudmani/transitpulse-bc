-- Verification queries used throughout the build. Run the matching one at
-- the end of each phase rather than assuming a step worked.

-- Phase 2: is bronze landing?
SELECT dt, hour, count(*) AS rows, count(DISTINCT trip_id) AS trips,
       count(arrival_delay) AS labelled
FROM transitpulse.bronze_trip_updates
WHERE dt = date_format(current_date, '%Y-%m-%d')
GROUP BY dt, hour
ORDER BY hour;

-- Phase 3a: do the realtime and static trip_id formats actually match?
-- If this returns 0 the whole pipeline is broken and everything downstream
-- will silently produce nothing.
SELECT count(*) AS joined_rows
FROM transitpulse.bronze_trip_updates t
JOIN transitpulse.dim_trips d ON t.trip_id = d.trip_id
WHERE t.dt = date_format(current_date, '%Y-%m-%d');

-- Phase 3b: silver dedupe correctness. dupes MUST be 0.
SELECT
  count(*)                                                   AS events,
  count(observed_delay_sec)                                  AS labelled,
  count(*) - count(DISTINCT trip_id || '|' || stop_id)       AS dupes,
  approx_percentile(observed_delay_sec, 0.5)                 AS median_delay
FROM transitpulse.stop_events
WHERE service_date = current_date - interval '1' day;

-- Phase 3c: coverage across the backfill. A day with 10% of normal volume
-- means the poller was down; do not train on it.
SELECT service_date, count(*) AS events
FROM transitpulse.stop_events
GROUP BY service_date
ORDER BY service_date;

-- Phase 4: feature completeness before training.
SELECT
  count(*)                                                             AS rows,
  count(DISTINCT service_date)                                         AS days,
  sum(CASE WHEN hist_median_delay IS NULL THEN 1 ELSE 0 END)           AS missing_prior,
  sum(CASE WHEN delay_t_minus_15 IS NULL THEN 1 ELSE 0 END)            AS missing_snapshot
FROM transitpulse.training_features;
