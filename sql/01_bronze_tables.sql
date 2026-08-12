-- Bronze tables use Athena partition projection: no crawler runs, no
-- MSCK REPAIR, no partition metadata drift, and no crawler cost.
-- Replace ${BUCKET} and ${START_DATE} before running.

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.bronze_trip_updates (
  record_type           string,
  feed_timestamp        bigint,
  ingest_ts             bigint,
  trip_id               string,
  route_id              string,
  direction_id          int,
  start_date            string,
  schedule_relationship int,
  vehicle_id            string,
  stop_id               string,
  stop_sequence         int,
  arrival_time          bigint,
  arrival_delay         int,
  departure_time        bigint,
  departure_delay       int
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/raw/trip_updates/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '${START_DATE},NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hour.type'        = 'integer',
  'projection.hour.range'       = '0,23',
  'projection.hour.digits'      = '2',
  'storage.location.template'   = 's3://${BUCKET}/raw/trip_updates/dt=${dt}/hour=${hour}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.bronze_vehicle_positions (
  record_type           string,
  feed_timestamp        bigint,
  ingest_ts             bigint,
  vehicle_id            string,
  trip_id               string,
  route_id              string,
  latitude              double,
  longitude             double,
  bearing               double,
  speed                 double,
  current_stop_sequence int,
  occupancy_status      int,
  vehicle_timestamp     bigint
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/raw/vehicle_positions/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '${START_DATE},NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hour.type'        = 'integer',
  'projection.hour.range'       = '0,23',
  'projection.hour.digits'      = '2',
  'storage.location.template'   = 's3://${BUCKET}/raw/vehicle_positions/dt=${dt}/hour=${hour}/'
);
