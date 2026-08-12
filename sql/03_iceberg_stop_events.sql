-- Silver stop events. Iceberg because late-arriving corrections must UPDATE
-- rows in place, and because snapshot ids make training sets reproducible.
-- Replace ${BUCKET} before running.

CREATE TABLE IF NOT EXISTS transitpulse.stop_events (
  service_date            date,
  trip_id                 string,
  stop_id                 string,
  route_id                string,
  direction_id            int,
  vehicle_id              string,
  stop_sequence           int,
  start_date              string,
  observed_arrival_epoch  bigint,
  observed_arrival_ts     timestamp,
  observed_delay_sec      int,
  last_seen_ts            bigint,
  delay_t_minus_5         int,
  delay_t_minus_15        int,
  delay_t_minus_30        int,
  snap_ts_t_minus_5       bigint,
  snap_ts_t_minus_15      bigint,
  snap_ts_t_minus_30      bigint,
  scheduled_arrival       string,
  shape_dist_traveled     double,
  stop_lat                double,
  stop_lon                double,
  route_short_name        string,
  route_type              int,
  hour_of_day             int,
  day_of_week             int,
  is_weekend              int,
  stops_remaining         int,
  is_terminus             int
)
PARTITIONED BY (month(service_date))
LOCATION 's3://${BUCKET}/iceberg/stop_events/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
