"""Bronze -> Silver: collapse repeated arrival predictions into stop events.

The realtime feed emits a prediction for the same stop many times as a bus
approaches. This job produces exactly one row per (service_date, trip_id,
stop_id) holding the final observed delay plus snapshots taken at fixed
prediction horizons, then MERGEs it into an Iceberg table so late-arriving
corrections update in place rather than duplicating.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ARGS = getResolvedOptions(
    sys.argv, ["JOB_NAME", "run_date", "bronze_db", "bronze_bucket", "silver_table"]
)

SC = SparkContext.getOrCreate()
GLUE = GlueContext(SC)
SPARK = GLUE.spark_session
JOB = Job(GLUE)
JOB.init(ARGS["JOB_NAME"], ARGS)


def resolve_run_date(raw: str) -> str:
    """Turn the --run_date argument into the service date to process.

    EventBridge passes a full ISO timestamp and the schedule fires at 02:20
    UTC, so a scheduled run must process the *previous* UTC day -- the current
    one is two hours old and would fail the DQ gate's row-count minimum.

    A bare YYYY-MM-DD (scripts/backfill.sh, manual runs) is used exactly as
    given, so replaying a specific day still means that day.
    """
    if len(raw) > 10:
        return (date.fromisoformat(raw[:10]) - timedelta(days=1)).isoformat()
    return raw


RUN_DATE = resolve_run_date(ARGS["run_date"])
BRONZE_DB = ARGS["bronze_db"]
BRONZE_BUCKET = ARGS["bronze_bucket"]
SILVER_TABLE = ARGS["silver_table"]

KEY = ["start_date", "trip_id", "stop_id"]
HORIZONS_MIN = (5, 15, 30)

DELAY_FLOOR_SEC = -1800
DELAY_CEILING_SEC = 7200


def read_trip_updates() -> DataFrame:
    """Read one day of bronze straight from S3, not through the Data Catalog.

    bronze_trip_updates uses Athena *partition projection*: partitions are
    computed from a formula at query time and never written to the catalog
    (`aws glue get-partitions` returns 0). That is an Athena-only feature --
    Spark asks the catalog for a partition list, gets an empty one, and reads
    zero files while the job still reports SUCCEEDED.

    The Firehose prefix is deterministic, so addressing it directly avoids the
    catalog entirely. The dim_* tables are unpartitioned and still resolve
    through the catalog normally.
    """
    return (
        SPARK.read.parquet(f"s3://{BRONZE_BUCKET}/raw/trip_updates/dt={RUN_DATE}/")
        .where(F.col("trip_id").isNotNull())
        .where(F.col("stop_id").isNotNull())
        .where(F.col("start_date").isNotNull())
    )


def final_observation(updates: DataFrame) -> DataFrame:
    """Keep the last prediction received for each stop event."""
    window = Window.partitionBy(*KEY).orderBy(F.col("ingest_ts").desc())
    return (
        updates.withColumn("rn", F.row_number().over(window))
        .where(F.col("rn") == 1)
        .select(
            *KEY,
            "route_id",
            "direction_id",
            "vehicle_id",
            "stop_sequence",
            F.col("arrival_time").alias("observed_arrival_epoch"),
            F.col("arrival_delay").alias("observed_delay_sec"),
            F.col("ingest_ts").alias("last_seen_ts"),
        )
    )


def snapshot_at(updates: DataFrame, minutes: int) -> DataFrame:
    """The prediction closest to exactly `minutes` before predicted arrival."""
    horizon = minutes * 60
    candidates = updates.where(
        (F.col("arrival_time") - F.col("ingest_ts")) >= F.lit(horizon)
    ).withColumn("gap", (F.col("arrival_time") - F.col("ingest_ts")) - F.lit(horizon))
    window = Window.partitionBy(*KEY).orderBy(F.col("gap").asc())
    return (
        candidates.withColumn("rn", F.row_number().over(window))
        .where(F.col("rn") == 1)
        .select(
            *KEY,
            F.col("arrival_delay").alias(f"delay_t_minus_{minutes}"),
            F.col("ingest_ts").alias(f"snap_ts_t_minus_{minutes}"),
        )
    )


def join_dimensions(events: DataFrame) -> DataFrame:
    stop_times = SPARK.table(f"{BRONZE_DB}.dim_stop_times").select(
        F.col("trip_id"),
        F.col("stop_id"),
        F.col("stop_sequence").cast("int").alias("sched_stop_sequence"),
        F.col("arrival_time").alias("scheduled_arrival"),
        F.col("shape_dist_traveled").cast("double").alias("shape_dist_traveled"),
    )
    stops = SPARK.table(f"{BRONZE_DB}.dim_stops").select(
        F.col("stop_id"),
        F.col("stop_lat").cast("double").alias("stop_lat"),
        F.col("stop_lon").cast("double").alias("stop_lon"),
    )
    routes = SPARK.table(f"{BRONZE_DB}.dim_routes").select(
        F.col("route_id"),
        F.col("route_short_name"),
        F.col("route_type").cast("int").alias("route_type"),
    )
    return (
        events.join(stop_times, on=["trip_id", "stop_id"], how="left")
        .join(stops, on="stop_id", how="left")
        .join(routes, on="route_id", how="left")
    )


def derive(events: DataFrame) -> DataFrame:
    trip_window = Window.partitionBy("start_date", "trip_id")
    return (
        events.withColumn("service_date", F.to_date(F.col("start_date"), "yyyyMMdd"))
        # timestamp_seconds, not to_timestamp. observed_arrival_epoch is a bigint
        # of epoch seconds; to_timestamp() expects a string and would implicitly
        # cast, fail to parse "1786479340" against yyyy-MM-dd HH:mm:ss, and return
        # NULL -- silently nulling hour_of_day, day_of_week, is_weekend and
        # minutes_since_service_start, with the job still reporting success.
        .withColumn("observed_arrival_ts", F.timestamp_seconds(F.col("observed_arrival_epoch")))
        .withColumn("hour_of_day", F.hour("observed_arrival_ts"))
        .withColumn("day_of_week", F.dayofweek("observed_arrival_ts"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn("max_stop_sequence", F.max("stop_sequence").over(trip_window))
        .withColumn("stops_remaining", F.col("max_stop_sequence") - F.col("stop_sequence"))
        .withColumn(
            "is_terminus",
            (F.col("stop_sequence") == F.col("max_stop_sequence")).cast("int"),
        )
        # Beyond these bounds it is a data artefact, not a late bus.
        .withColumn(
            "observed_delay_sec",
            F.when(
                (F.col("observed_delay_sec") < F.lit(DELAY_FLOOR_SEC))
                | (F.col("observed_delay_sec") > F.lit(DELAY_CEILING_SEC)),
                F.lit(None).cast("int"),
            ).otherwise(F.col("observed_delay_sec")),
        )
        .drop("max_stop_sequence", "sched_stop_sequence")
    )


def main() -> None:
    updates = read_trip_updates()
    events = final_observation(updates)

    for minutes in HORIZONS_MIN:
        events = events.join(snapshot_at(updates, minutes), on=KEY, how="left")

    events = derive(join_dimensions(events))
    events = events.dropDuplicates(["service_date", "trip_id", "stop_id"])
    events.createOrReplaceTempView("staged_stop_events")

    # MERGE rather than INSERT: a stop event seen again in a later run should
    # update, not duplicate. This is why silver is Iceberg.
    SPARK.sql(
        f"""
        MERGE INTO {SILVER_TABLE} AS t
        USING staged_stop_events AS s
           ON t.service_date = s.service_date
          AND t.trip_id      = s.trip_id
          AND t.stop_id      = s.stop_id
        WHEN MATCHED AND s.last_seen_ts > t.last_seen_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    written = events.count()
    print(f'{{"metric": "silver_rows", "value": {written}, "run_date": "{RUN_DATE}"}}')
    JOB.commit()


if __name__ == "__main__":
    main()
