-- Queries behind the README charts and the baseline table.
--
-- The CSVs they produce live in data/processed/, which is gitignored -- derived
-- data is regenerable and does not belong in history. These queries are the
-- reproducible part.
--
-- Run each with scripts/athena_query.sh, which saves the result under a name you
-- choose instead of the UUID Athena assigns:
--
--   ./scripts/athena_query.sh delay_distribution <<'SQL'
--   ...paste query 1...
--   SQL
--
-- Then regenerate the figures:
--   python scripts/plot_profile.py


-- ---------------------------------------------------------------------------
-- 1. delay_distribution.csv  ->  img/delay_distribution.png
--
-- Buckets are asymmetric on purpose: "on time" is +/-1 minute, and the late side
-- is split more finely because that is the tail the model is judged on. A rider
-- cares about the difference between 3 and 10 minutes late; nobody cares about
-- the difference between 3 and 10 minutes early.
-- ---------------------------------------------------------------------------
SELECT
  CASE
    WHEN observed_delay_sec < -300 THEN '1. early >5m'
    WHEN observed_delay_sec < -60  THEN '2. early 1-5m'
    WHEN observed_delay_sec < 60   THEN '3. on time'
    WHEN observed_delay_sec < 180  THEN '4. late 1-3m'
    WHEN observed_delay_sec < 300  THEN '5. late 3-5m'
    WHEN observed_delay_sec < 600  THEN '6. late 5-10m'
    ELSE '7. late >10m'
  END AS bucket,
  count(*) AS n
FROM transitpulse.stop_events
WHERE observed_delay_sec IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- 2. delay_by_hour.csv  ->  img/hourly_profile.png
--
-- hour_of_day is UTC: it comes from observed_arrival_ts, which is built with
-- timestamp_seconds() over an epoch. scripts/plot_profile.py shifts it by -7 for
-- display only. The model's feature stays UTC -- which is the bug the README
-- calls out, since PEAK_HOURS was written for local hours.
-- ---------------------------------------------------------------------------
SELECT hour_of_day,
       count(*)                                        AS events,
       round(avg(observed_delay_sec), 1)               AS avg_delay,
       round(approx_percentile(observed_delay_sec, 0.9), 1) AS p90_delay
FROM transitpulse.stop_events
WHERE observed_delay_sec IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ---------------------------------------------------------------------------
-- 3. The baseline table in the README.
--
-- mae_schedule is what the printed timetable achieves: predict zero delay.
-- mae_persistence assumes the bus stays exactly as late as it was 15 minutes
-- before arrival. coalesce to 0 so a missing snapshot degrades to the schedule
-- baseline rather than dropping the row and flattering the number.
--
-- The registry gate is mae_ratio_vs_persistence <= 0.92, so a model must reach
-- mae_persistence * 0.92 to be registered at all.
-- ---------------------------------------------------------------------------
SELECT count(*)                                                       AS n,
       round(avg(abs(observed_delay_sec)), 1)                         AS mae_schedule,
       round(avg(abs(observed_delay_sec - coalesce(delay_t_minus_15, 0))), 1) AS mae_persistence
FROM transitpulse.stop_events
WHERE observed_delay_sec IS NOT NULL;


-- ---------------------------------------------------------------------------
-- 4. Once >= 5 days are collected: the historical-median baseline.
--
-- Empty before then. hist_median_delay requires >= 20 observations per
-- route/stop/day-type/hour cell drawn from STRICTLY earlier service dates, which
-- is the leakage guard in gold_features.py. Needs sql/06_gold_tables.sql to have
-- been run, since gold writes plain Parquet and registers no table.
-- ---------------------------------------------------------------------------
SELECT count(*)                                                          AS n,
       count(hist_median_delay)                                          AS with_prior,
       round(avg(abs(observed_delay_sec - coalesce(hist_median_delay, 0))), 1) AS mae_historical
FROM transitpulse.training_features
WHERE observed_delay_sec IS NOT NULL;
