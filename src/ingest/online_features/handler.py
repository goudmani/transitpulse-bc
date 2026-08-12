"""Maintain current route/stop delay state in DynamoDB from the Kinesis stream.

This is the second consumer of the stream and the reason the design uses
Kinesis Data Streams rather than Firehose alone. The serving Lambda reads
what this function writes, so its output must match the offline feature
definitions in src/common/features.py.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any

import boto3

LOG = logging.getLogger()
LOG.setLevel("INFO")

_table = None


def table():
    """Lazy so the module imports without AWS credentials or a region."""
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["ONLINE_TABLE"])
    return _table


def ttl_seconds() -> int:
    return int(os.environ.get("TTL_SECONDS", "7200"))


DELAY_FLOOR_SEC = -1800
DELAY_CEILING_SEC = 7200


def decode(record: dict[str, Any]) -> list[dict[str, Any]]:
    """One Kinesis record may hold several newline-delimited JSON rows."""
    raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            LOG.warning("skipping malformed row")
    return rows


def plausible(delay: Any) -> bool:
    if delay is None:
        return False
    try:
        value = float(delay)
    except (TypeError, ValueError):
        return False
    return DELAY_FLOOR_SEC <= value <= DELAY_CEILING_SEC


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    now = int(time.time())
    ttl = now + ttl_seconds()

    # Aggregate the whole batch in memory, then write once per key. Writing
    # per record would multiply the DynamoDB bill for no benefit.
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    route_delays: dict[str, list[float]] = defaultdict(list)
    route_vehicles: dict[str, set[str]] = defaultdict(set)

    for record in event.get("Records", []):
        for row in decode(record):
            route_id = row.get("route_id")
            if not route_id:
                continue

            if row.get("record_type") == "vehicle_positions":
                if row.get("vehicle_id"):
                    route_vehicles[route_id].add(str(row["vehicle_id"]))
                continue

            stop_id = row.get("stop_id")
            delay = row.get("arrival_delay")
            if not stop_id or not plausible(delay):
                continue

            key = (str(route_id), str(stop_id))
            ingest_ts = int(row.get("ingest_ts") or now)
            if key not in latest or ingest_ts >= latest[key]["ingest_ts"]:
                latest[key] = {
                    "ingest_ts": ingest_ts,
                    "delay": float(delay),
                    "trip_id": row.get("trip_id"),
                    "stop_sequence": row.get("stop_sequence"),
                }
            route_delays[str(route_id)].append(float(delay))

    with table().batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for (route_id, stop_id), state in latest.items():
            batch.put_item(
                Item={
                    "pk": f"ROUTE#{route_id}#STOP#{stop_id}",
                    "sk": "CURRENT",
                    "current_delay": Decimal(str(round(state["delay"], 3))),
                    "trip_id": state.get("trip_id") or "",
                    "stop_sequence": Decimal(str(state.get("stop_sequence") or 0)),
                    "updated_at": Decimal(str(state["ingest_ts"])),
                    "ttl": Decimal(str(ttl)),
                }
            )

        for route_id, delays in route_delays.items():
            mean_delay = sum(delays) / len(delays)
            batch.put_item(
                Item={
                    "pk": f"ROUTE#{route_id}",
                    "sk": "ROUTESTATE",
                    "mean_route_delay_15m": Decimal(str(round(mean_delay, 3))),
                    "vehicles_active_on_route": Decimal(str(len(route_vehicles.get(route_id, ())))),
                    "updated_at": Decimal(str(now)),
                    "ttl": Decimal(str(ttl)),
                }
            )

    LOG.info(
        json.dumps(
            {
                "metric": "online_features_written",
                "stops": len(latest),
                "routes": len(route_delays),
            }
        )
    )
    return {"stops": len(latest), "routes": len(route_delays)}
