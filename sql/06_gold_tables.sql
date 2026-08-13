-- Gold training features.
--
-- src/glue/gold_features.py writes plain Parquet to s3://<gold>/features/training/
-- partitioned by service_date, and never registers a table. sql/04_baselines.sql
-- and sql/05_checks.sql both query transitpulse.training_features, so without
-- this DDL those queries fail with "table does not exist".
--
-- Uses partition projection rather than MSCK REPAIR so new days appear without
-- maintenance. Unlike bronze, nothing reads this table from Spark, so the
-- Athena-only nature of projection is not a problem here.
--
-- NOTE: Athena rejects -- comments inside the parenthesised column list, so the
-- column grouping is documented here instead:
--   identity          trip_id .. start_date
--   label             observed_delay_sec .. last_seen_ts
--   snapshots         delay_t_minus_* and snap_ts_t_minus_*
--   temporal          hour_of_day .. minutes_since_service_start
--   geometry          stop_sequence .. stop_lon
--   historical prior  hist_* (28-day trailing, strictly earlier service dates)
--   live state        prev_stop_delay .. vehicles_active_on_route
--   weather           temp_c .. visibility_m (constants until dim_weather exists)
--   disruption        active_alert_on_route (hardcoded 0 until alerts are ingested)
--
-- Replace ${BUCKET} with transitpulse-gold-<account> and ${START_DATE} with the
-- first service date collected.

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.training_features (
  trip_id                     string,
  stop_id                     string,
  route_id                    string,
  direction_id                int,
  vehicle_id                  string,
  start_date                  string,
  observed_delay_sec          int,
  observed_arrival_epoch      bigint,
  observed_arrival_ts         timestamp,
  last_seen_ts                bigint,
  delay_t_minus_5             int,
  delay_t_minus_15            int,
  delay_t_minus_30            int,
  snap_ts_t_minus_5           bigint,
  snap_ts_t_minus_15          bigint,
  snap_ts_t_minus_30          bigint,
  hour_of_day                 int,
  day_of_week                 int,
  is_weekend                  int,
  is_holiday                  int,
  is_peak                     int,
  minutes_since_service_start int,
  stop_sequence               int,
  stops_remaining             int,
  shape_dist_traveled         double,
  is_terminus                 int,
  route_type                  int,
  route_short_name            string,
  scheduled_arrival           string,
  stop_lat                    double,
  stop_lon                    double,
  hist_median_delay           int,
  hist_p90_delay              int,
  hist_std_delay              double,
  hist_n                      bigint,
  prev_stop_delay             int,
  upstream_delay_same_trip    double,
  preceding_trip_delay        int,
  mean_route_delay_15m        double,
  vehicles_active_on_route    bigint,
  temp_c                      double,
  precipitation_mm            double,
  wind_kph                    double,
  visibility_m                double,
  active_alert_on_route       int
)
PARTITIONED BY (service_date date)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/features/training/'
TBLPROPERTIES (
  'projection.enabled'                    = 'true',
  'projection.service_date.type'          = 'date',
  'projection.service_date.range'         = '${START_DATE},NOW',
  'projection.service_date.format'        = 'yyyy-MM-dd',
  'projection.service_date.interval'      = '1',
  'projection.service_date.interval.unit' = 'DAYS',
  'storage.location.template'             = 's3://${BUCKET}/features/training/service_date=${service_date}/'
);
