-- Static GTFS dimensions. skip.header.line.count drops the CSV header row.
-- Replace ${BUCKET} and ${VERSION} before running.

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.dim_routes (
  route_id         string,
  agency_id        string,
  route_short_name string,
  route_long_name  string,
  route_desc       string,
  route_type       string,
  route_url        string,
  route_color      string,
  route_text_color string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://${BUCKET}/static/gtfs/version=${VERSION}/routes/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.dim_trips (
  route_id      string,
  service_id    string,
  trip_id       string,
  trip_headsign string,
  direction_id  string,
  block_id      string,
  shape_id      string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://${BUCKET}/static/gtfs/version=${VERSION}/trips/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.dim_stops (
  stop_id        string,
  stop_code      string,
  stop_name      string,
  stop_desc      string,
  stop_lat       string,
  stop_lon       string,
  zone_id        string,
  stop_url       string,
  location_type  string,
  parent_station string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://${BUCKET}/static/gtfs/version=${VERSION}/stops/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.dim_stop_times (
  trip_id             string,
  arrival_time        string,
  departure_time      string,
  stop_id             string,
  stop_sequence       string,
  stop_headsign       string,
  pickup_type         string,
  drop_off_type       string,
  shape_dist_traveled string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://${BUCKET}/static/gtfs/version=${VERSION}/stop_times/'
TBLPROPERTIES ('skip.header.line.count' = '1');
