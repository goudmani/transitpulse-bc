"""Join captured predictions to observed outcomes and publish rolling metrics.

Ground truth arrives hours after the prediction, so this runs daily over a
lagged window. It emits the three numbers that make the project defensible:
the model's MAE, the persistence baseline, and the schedule baseline.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "silver_table", "gold_bucket"])

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
SILVER_TABLE = ARGS["silver_table"]
GOLD_BUCKET = ARGS["gold_bucket"]

NAMESPACE = "TransitPulse"


def main() -> None:
    predictions = SPARK.read.json(f"s3://{GOLD_BUCKET}/capture/**/{RUN_DATE}/*").select(
        F.col("route_id").cast("string").alias("route_id"),
        F.col("stop_id").cast("string").alias("stop_id"),
        F.col("trip_id").cast("string").alias("trip_id"),
        F.to_date(F.col("service_date")).alias("service_date"),
        F.col("predicted_delay_sec").cast("double").alias("predicted_delay_sec"),
        F.col("delay_t_minus_15").cast("double").alias("delay_t_minus_15"),
    )

    actuals = SPARK.table(SILVER_TABLE).select(
        "service_date",
        "trip_id",
        "stop_id",
        F.col("observed_delay_sec").cast("double").alias("observed_delay_sec"),
    )

    joined = predictions.join(
        actuals, on=["service_date", "trip_id", "stop_id"], how="inner"
    ).where(F.col("observed_delay_sec").isNotNull())

    metrics = joined.agg(
        F.count(F.lit(1)).alias("n"),
        F.avg(F.abs(F.col("observed_delay_sec") - F.col("predicted_delay_sec"))).alias("model_mae"),
        F.avg(
            F.abs(F.col("observed_delay_sec") - F.coalesce("delay_t_minus_15", F.lit(0.0)))
        ).alias("persistence_mae"),
        F.avg(F.abs(F.col("observed_delay_sec"))).alias("schedule_mae"),
        F.sqrt(F.avg(F.pow(F.col("observed_delay_sec") - F.col("predicted_delay_sec"), 2))).alias(
            "model_rmse"
        ),
    ).collect()[0]

    n = int(metrics["n"])
    if n == 0:
        print('{"warn": "no matched predictions for this run_date"}')
        JOB.commit()
        return

    model_mae = float(metrics["model_mae"])
    persistence_mae = float(metrics["persistence_mae"])
    schedule_mae = float(metrics["schedule_mae"])
    ratio = model_mae / persistence_mae if persistence_mae else float("nan")

    payload = {
        "run_date": RUN_DATE,
        "n": n,
        "model_mae": round(model_mae, 3),
        "persistence_mae": round(persistence_mae, 3),
        "schedule_mae": round(schedule_mae, 3),
        "model_rmse": round(float(metrics["model_rmse"]), 3),
        "mae_ratio_vs_persistence": round(ratio, 4),
    }

    boto3.client("s3").put_object(
        Bucket=GOLD_BUCKET,
        Key=f"metrics/backtest/dt={RUN_DATE}/metrics.json",
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    boto3.client("cloudwatch").put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[
            {"MetricName": "ModelMae", "Value": model_mae, "Unit": "Seconds"},
            {
                "MetricName": "PersistenceMae",
                "Value": persistence_mae,
                "Unit": "Seconds",
            },
            {"MetricName": "ScheduleMae", "Value": schedule_mae, "Unit": "Seconds"},
            {
                "MetricName": "ModelMaeRatioVsPersistence",
                "Value": ratio,
                "Unit": "None",
            },
            {"MetricName": "PredictionsScored", "Value": float(n), "Unit": "Count"},
        ],
    )

    print(json.dumps(payload))
    JOB.commit()


if __name__ == "__main__":
    main()
