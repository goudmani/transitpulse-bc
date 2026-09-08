"""Gold -> train/validation/test, split by time.

The split is chronological, never random. Gold carries historical aggregates
(hist_median_delay and friends) computed over a trailing window, so a row from
2026-08-20 already summarises the days around it. Shuffle the rows and those
aggregates carry the future into the training set: the metrics improve, and they
mean nothing. Holding out the *last* days is the only split that answers the
question the model is actually asked -- predict a day you have never seen.

Boundaries are derived from the data rather than hardcoded, so re-running after
more collection re-cuts the split instead of silently keeping a stale one.
"""

from __future__ import annotations

import sys
from datetime import timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "gold_bucket"])

SC = SparkContext.getOrCreate()
GLUE = GlueContext(SC)
SPARK = GLUE.spark_session
JOB = Job(GLUE)
JOB.init(ARGS["JOB_NAME"], ARGS)

GOLD_BUCKET = ARGS["gold_bucket"]

# Seven days each. Long enough that a validation or test score is not one bad
# Tuesday, short enough to leave the majority of a 27-day window for training.
# Both windows contain a weekend, which matters because is_weekend is a feature.
TEST_DAYS = 7
VAL_DAYS = 7

# Fewer, larger Parquet files. SageMaker downloads the whole channel before
# training starts, and many small objects make that slower for no benefit.
PARTITIONS = 8


def write_split(name: str, df: DataFrame) -> int:
    """Write one channel and return its row count.

    mode("overwrite") is safe here in a way it is not in gold_features.py: each
    channel has its own prefix and there is no partitionBy, so the overwrite
    scope is exactly the directory intended.
    """
    rows = df.count()
    (
        df.repartition(PARTITIONS)
        .write.mode("overwrite")
        .parquet(f"s3://{GOLD_BUCKET}/features/split/{name}/")
    )
    return rows


def main() -> None:
    gold = SPARK.read.parquet(f"s3://{GOLD_BUCKET}/features/training/")

    bounds = gold.agg(
        F.min("service_date").alias("first"), F.max("service_date").alias("last")
    ).collect()[0]
    first, last = bounds["first"], bounds["last"]

    val_end = last - timedelta(days=TEST_DAYS)
    train_end = val_end - timedelta(days=VAL_DAYS)

    if train_end <= first:
        raise SystemExit(
            f"Not enough history: gold spans {first}..{last}, which leaves no "
            f"training days before {train_end}. Collect more, or reduce "
            f"TEST_DAYS/VAL_DAYS."
        )

    train = gold.where(F.col("service_date") <= F.lit(train_end))
    val = gold.where(
        (F.col("service_date") > F.lit(train_end)) & (F.col("service_date") <= F.lit(val_end))
    )
    test = gold.where(F.col("service_date") > F.lit(val_end))

    counts = {
        "train": write_split("train", train),
        "val": write_split("val", val),
        "test": write_split("test", test),
    }

    total = gold.count()
    if sum(counts.values()) != total:
        raise SystemExit(
            f"Split lost or duplicated rows: {sum(counts.values())} across "
            f"channels vs {total} in gold. The boundary conditions overlap or "
            f"leave a gap."
        )

    print(
        '{"metric": "split_rows", '
        f'"train": {counts["train"]}, "val": {counts["val"]}, "test": {counts["test"]}, '
        f'"train_end": "{train_end}", "val_end": "{val_end}", '
        f'"first": "{first}", "last": "{last}"}}'
    )
    JOB.commit()


if __name__ == "__main__":
    main()
