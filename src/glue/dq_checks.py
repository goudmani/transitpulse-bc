"""Data quality gate. Exits non-zero so Step Functions routes to quarantine.

The point is not to crash the pipeline. It is to stop bad data reaching the
gold layer and poisoning every model trained afterwards, while leaving the
partition inspectable.
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

MIN_ROWS = 50_000
MIN_LABEL_COMPLETENESS = 0.85
MIN_UNIQUENESS = 0.999
DELAY_FLOOR_SEC = -1800
DELAY_CEILING_SEC = 7200


def main() -> None:
    df = SPARK.table(SILVER_TABLE).where(F.col("service_date") == F.lit(RUN_DATE).cast("date"))

    stats = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count("trip_id").alias("trip_ids"),
        F.count("observed_delay_sec").alias("labelled"),
        F.countDistinct("service_date", "trip_id", "stop_id").alias("distinct_keys"),
        F.min("observed_delay_sec").alias("min_delay"),
        F.max("observed_delay_sec").alias("max_delay"),
    ).collect()[0]

    rows = int(stats["rows"])
    results = []

    def check(name: str, passed: bool, observed, expected) -> None:
        results.append(
            {
                "rule": name,
                "passed": bool(passed),
                "observed": observed,
                "expected": expected,
            }
        )

    check("row_count_minimum", rows >= MIN_ROWS, rows, f">= {MIN_ROWS}")
    check(
        "trip_id_completeness",
        rows > 0 and stats["trip_ids"] == rows,
        stats["trip_ids"],
        rows,
    )

    completeness = (stats["labelled"] / rows) if rows else 0.0
    check(
        "label_completeness",
        completeness >= MIN_LABEL_COMPLETENESS,
        round(completeness, 4),
        f">= {MIN_LABEL_COMPLETENESS}",
    )

    uniqueness = (stats["distinct_keys"] / rows) if rows else 0.0
    check(
        "key_uniqueness",
        uniqueness >= MIN_UNIQUENESS,
        round(uniqueness, 6),
        f">= {MIN_UNIQUENESS}",
    )

    in_range = rows == 0 or (
        stats["min_delay"] is None
        or (stats["min_delay"] >= DELAY_FLOOR_SEC and stats["max_delay"] <= DELAY_CEILING_SEC)
    )
    check(
        "delay_within_bounds",
        in_range,
        [stats["min_delay"], stats["max_delay"]],
        [DELAY_FLOOR_SEC, DELAY_CEILING_SEC],
    )

    payload = json.dumps({"run_date": RUN_DATE, "rows": rows, "results": results}, default=str)
    boto3.client("s3").put_object(
        Bucket=GOLD_BUCKET,
        Key=f"dq/dt={RUN_DATE}/results.json",
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    print(payload)

    failed = [r for r in results if not r["passed"]]
    if failed:
        print(f'{{"metric": "dq_failed", "value": {len(failed)}}}')
        JOB.commit()
        sys.exit(1)

    print('{"metric": "dq_failed", "value": 0}')
    JOB.commit()


if __name__ == "__main__":
    main()
