"""HTTP prediction handler: DynamoDB feature lookup + SageMaker invoke.

Imports the same feature contract the training job uses (features.py is
vendored into this package at build time) so the online and offline paths
cannot drift apart silently.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError
from features import FEATURE_ORDER, build_feature_vector, is_peak_hour, to_csv_row

LOG = logging.getLogger()
LOG.setLevel("INFO")

_table = None
_runtime = None
_cloudwatch = None


def table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["ONLINE_TABLE"])
    return _table


def runtime_client():
    global _runtime
    if _runtime is None:
        _runtime = boto3.client("sagemaker-runtime")
    return _runtime


def cloudwatch_client():
    global _cloudwatch
    if _cloudwatch is None:
        _cloudwatch = boto3.client("cloudwatch")
    return _cloudwatch


def endpoint_name() -> str:
    return os.environ["ENDPOINT_NAME"]


def metric_namespace() -> str:
    return os.environ.get("METRIC_NS", "TransitPulse")


# Vancouver is UTC-7 in summer, UTC-8 in winter. Fixed offset is adequate for
# hour-of-day bucketing and avoids a tzdata dependency in the Lambda zip.
LOCAL_OFFSET = timedelta(hours=-7)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_item(pk: str, sk: str) -> dict[str, Any]:
    try:
        response = table().get_item(Key={"pk": pk, "sk": sk})
    except ClientError as exc:
        LOG.warning("dynamodb get_item failed for %s/%s: %s", pk, sk, exc)
        return {}
    return response.get("Item") or {}


def build_features(route_id: str, stop_id: str, now: datetime) -> dict[str, Any]:
    local = now + LOCAL_OFFSET
    hour = local.hour
    dow = (local.weekday() + 1) % 7 + 1  # match Spark's dayofweek: Sunday = 1

    current = get_item(f"ROUTE#{route_id}#STOP#{stop_id}", "CURRENT")
    route = get_item(f"ROUTE#{route_id}", "ROUTESTATE")
    stats = get_item(
        f"ROUTE#{route_id}#STOP#{stop_id}",
        f"STATS#{1 if dow in (1, 7) else 0}#{hour:02d}",
    )

    return {
        "hour_of_day": hour,
        "day_of_week": dow,
        "is_weekend": 1 if dow in (1, 7) else 0,
        "is_holiday": 0,
        "is_peak": is_peak_hour(hour),
        "minutes_since_service_start": hour * 60 + local.minute,
        "stop_sequence": _num(current.get("stop_sequence")),
        "stops_remaining": _num(stats.get("stops_remaining")),
        "shape_dist_traveled": _num(stats.get("shape_dist_traveled")),
        "is_terminus": _num(stats.get("is_terminus")),
        "route_type": _num(stats.get("route_type")),
        "hist_median_delay": _num(stats.get("hist_median_delay")),
        "hist_p90_delay": _num(stats.get("hist_p90_delay")),
        "hist_std_delay": _num(stats.get("hist_std_delay")),
        "hist_n": _num(stats.get("hist_n")),
        "delay_t_minus_15": _num(current.get("current_delay")),
        "prev_stop_delay": _num(current.get("prev_stop_delay")),
        "upstream_delay_same_trip": _num(current.get("upstream_delay")),
        "preceding_trip_delay": _num(route.get("preceding_trip_delay")),
        "mean_route_delay_15m": _num(route.get("mean_route_delay_15m")),
        "vehicles_active_on_route": _num(route.get("vehicles_active_on_route")),
        "temp_c": _num(route.get("temp_c")),
        "precipitation_mm": _num(route.get("precipitation_mm")),
        "wind_kph": _num(route.get("wind_kph")),
        "visibility_m": _num(route.get("visibility_m")),
        "active_alert_on_route": _num(route.get("active_alert_on_route")),
    }


def invoke_model(vector: list[float]) -> float:
    response = runtime_client().invoke_endpoint(
        EndpointName=endpoint_name(),
        ContentType="text/csv",
        Body=to_csv_row(vector).encode("utf-8"),
    )
    body = response["Body"].read().decode("utf-8").strip()
    return float(body.splitlines()[0].split(",")[0])


def respond(status: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(payload),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    started = time.perf_counter()
    params = event.get("queryStringParameters") or {}
    route_id = params.get("route_id")
    stop_id = params.get("stop_id")

    if not route_id or not stop_id:
        return respond(400, {"error": "route_id and stop_id are required"})

    horizon_min = int(params.get("horizon_min", 15))
    now = datetime.now(UTC)

    source = build_features(str(route_id), str(stop_id), now)
    vector = build_feature_vector(source)

    try:
        predicted = invoke_model(vector)
    except ClientError as exc:
        LOG.error("endpoint invoke failed: %s", exc)
        return respond(503, {"error": "model endpoint unavailable"})

    baseline = source.get("delay_t_minus_15")
    known_features = sum(1 for name in FEATURE_ORDER if source.get(name) is not None)

    latency_ms = (time.perf_counter() - started) * 1000
    cloudwatch_client().put_metric_data(
        Namespace=metric_namespace(),
        MetricData=[
            {"MetricName": "PredictLatencyMs", "Value": latency_ms, "Unit": "Milliseconds"},
            {
                "MetricName": "FeatureFillRate",
                "Value": known_features / len(FEATURE_ORDER),
                "Unit": "None",
            },
        ],
    )

    return respond(
        200,
        {
            "route_id": route_id,
            "stop_id": stop_id,
            "horizon_min": horizon_min,
            "predicted_delay_sec": round(predicted, 1),
            "baseline_delay_sec": round(baseline, 1) if baseline is not None else None,
            "feature_fill_rate": round(known_features / len(FEATURE_ORDER), 3),
            "model_endpoint": endpoint_name(),
            "generated_at": now.isoformat(),
        },
    )
