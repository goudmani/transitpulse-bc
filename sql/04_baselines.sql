-- Run this BEFORE training anything. These three numbers are what the model
-- must beat, and they belong in docs/baselines.md and in the README.

SELECT
  count(*)                                                       AS n,
  avg(abs(observed_delay_sec))                                   AS mae_schedule,
  avg(abs(observed_delay_sec - coalesce(delay_t_minus_15, 0)))   AS mae_persistence,
  avg(abs(observed_delay_sec - coalesce(hist_median_delay, 0)))  AS mae_historical
FROM transitpulse.training_features
WHERE service_date >= current_date - interval '14' day;
