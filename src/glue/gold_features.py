"""Silver -> Gold: build the point-in-time-correct training feature table.

The rule this job exists to enforce: every feature must have been knowable at
snapshot time (15 minutes before predicted arrival). Historical aggregates use
service dates strictly earlier than the run date. Breaking that rule leaks the
label and produces metrics that look excellent and mean nothing.
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
    sys.argv, ["JOB_NAME", "run_date", "glue_db", "silver_table", "gold_bucket"]
)

SC = SparkContext.getOrCreate()
GLUE = GlueContext(SC)
SPARK = GLUE.spark_session
JOB = Job(GLUE)
JOB.init(ARGS["JOB_NAME"], ARGS)


def resolve_run_date(raw: str) -> str:
    """Scheduled runs (full ISO timestamp) process the previous UTC day;
    explicit YYYY-MM-DD arguments are used as given. Must match the same
    helper in the other Glue jobs or the state machine's three steps would
    disagree about which day they are working on.
    """
    if len(raw) > 10:
        return (date.fromisoformat(raw[:10]) - timedelta(days=1)).isoformat()
    return raw


RUN_DATE = resolve_run_date(ARGS["run_date"])
GLUE_DB = ARGS["glue_db"]
SILVER_TABLE = ARGS["silver_table"]
GOLD_BUCKET = ARGS["gold_bucket"]

LOOKBACK_DAYS = 28
MIN_CELL_COUNT = 20  # do not trust an aggregate built from a handful of rows
PEAK_HOURS = [7, 8, 15, 16, 17]


def read_silver() -> DataFrame:
    return SPARK.table(SILVER_TABLE)


def historical_priors(silver: DataFrame) -> DataFrame:
    """Trailing delay profile per route/stop/day-type/hour.

    Strictly earlier than RUN_DATE. This filter is the leakage guard and is
    asserted by tests/unit/test_features.py.
    """
    window_start = F.date_sub(F.lit(RUN_DATE).cast("date"), LOOKBACK_DAYS)
    return (
        silver.where(F.col("service_date") < F.lit(RUN_DATE).cast("date"))
        .where(F.col("service_date") >= window_start)
        .where(F.col("observed_delay_sec").isNotNull())
        .groupBy("route_id", "stop_id", "is_weekend", "hour_of_day")
        .agg(
            F.expr("percentile_approx(observed_delay_sec, 0.5)").alias("hist_median_delay"),
            F.expr("percentile_approx(observed_delay_sec, 0.9)").alias("hist_p90_delay"),
            F.stddev("observed_delay_sec").alias("hist_std_delay"),
            F.count(F.lit(1)).alias("hist_n"),
        )
        .where(F.col("hist_n") >= MIN_CELL_COUNT)
    )


def upstream_features(today: DataFrame) -> DataFrame:
    """Delay at the previous stop on the same trip, and on the preceding trip."""
    trip_window = (
        Window.partitionBy("service_date", "trip_id")
        .orderBy("stop_sequence")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prev_stop = Window.partitionBy("service_date", "trip_id").orderBy("stop_sequence")
    route_window = Window.partitionBy("service_date", "route_id", "stop_id").orderBy(
        "observed_arrival_epoch"
    )

    return (
        today.withColumn("prev_stop_delay", F.lag("delay_t_minus_15", 1).over(prev_stop))
        .withColumn("upstream_delay_same_trip", F.avg("delay_t_minus_15").over(trip_window))
        .withColumn("preceding_trip_delay", F.lag("delay_t_minus_15", 1).over(route_window))
    )


def route_state(today: DataFrame) -> DataFrame:
    route_hour = Window.partitionBy("service_date", "route_id", "hour_of_day")
    return today.withColumn(
        "mean_route_delay_15m", F.avg("delay_t_minus_15").over(route_hour)
    ).withColumn(
        "vehicles_active_on_route",
        F.approx_count_distinct("vehicle_id").over(route_hour),
    )


def join_weather(today: DataFrame) -> DataFrame:
    weather = SPARK.table(f"{GLUE_DB}.dim_weather").select(
        F.to_date(F.col("time")).alias("weather_date"),
        F.hour(F.col("time")).alias("weather_hour"),
        F.col("temperature_2m").cast("double").alias("temp_c"),
        F.col("precipitation").cast("double").alias("precipitation_mm"),
        F.col("wind_speed_10m").cast("double").alias("wind_kph"),
        F.col("visibility").cast("double").alias("visibility_m"),
    )
    return today.join(
        weather,
        (today.service_date == weather.weather_date) & (today.hour_of_day == weather.weather_hour),
        how="left",
    ).drop("weather_date", "weather_hour")


def main() -> None:
    silver = read_silver()
    priors = historical_priors(silver)

    today = silver.where(F.col("service_date") == F.lit(RUN_DATE).cast("date"))
    today = upstream_features(today)
    today = route_state(today)

    features = (
        today.join(priors, on=["route_id", "stop_id", "is_weekend", "hour_of_day"], how="left")
        .withColumn("is_peak", F.col("hour_of_day").isin(PEAK_HOURS).cast("int"))
        .withColumn("is_holiday", F.lit(0))
        .withColumn(
            "minutes_since_service_start",
            F.col("hour_of_day") * F.lit(60) + F.minute(F.col("observed_arrival_ts")),
        )
        .withColumn("active_alert_on_route", F.lit(0))
    )

    try:
        features = join_weather(features)
    except Exception as exc:  # noqa: BLE001 - weather is optional enrichment
        print(f'{{"warn": "weather join skipped", "reason": "{exc}"}}')
        for column, default in (
            ("temp_c", 10.0),
            ("precipitation_mm", 0.0),
            ("wind_kph", 10.0),
            ("visibility_m", 20000.0),
        ):
            features = features.withColumn(column, F.lit(default))

    features = features.where(F.col("observed_delay_sec").isNotNull())

    # STATIC is Spark's default partition-overwrite mode, and it deletes the
    # ENTIRE table path before writing -- so every nightly run was wiping each
    # previous service_date and leaving only the day it had just processed.
    # Silver never showed this because Iceberg MERGE accumulates; gold is plain
    # Parquet and had been quietly rebuilding itself from scratch every night.
    # DYNAMIC replaces only the partitions present in this DataFrame.
    SPARK.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        features.repartition(4)
        .write.mode("overwrite")
        .partitionBy("service_date")
        .parquet(f"s3://{GOLD_BUCKET}/features/training/")
    )

    count = features.count()
    print(f'{{"metric": "gold_rows", "value": {count}, "run_date": "{RUN_DATE}"}}')
    JOB.commit()


if __name__ == "__main__":
    main()
