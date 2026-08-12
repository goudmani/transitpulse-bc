"""Poll the TransLink GTFS-Realtime feeds and publish flattened rows to Kinesis.

Runs every minute on an EventBridge schedule. Deliberately not attached to a
VPC: it needs outbound internet, and a VPC-attached Lambda would require a
NAT Gateway costing more than the rest of the pipeline combined.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import requests
from botocore.config import Config
from google.transit import gtfs_realtime_pb2

LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_BOTO_CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"})

# Clients are created lazily rather than at import time. Eager construction
# needs a region and credentials just to import the module, which makes the
# pure flattening functions impossible to unit test.
_kinesis = None
_secrets = None


def kinesis_client():
    global _kinesis
    if _kinesis is None:
        _kinesis = boto3.client("kinesis", config=_BOTO_CFG)
    return _kinesis


def secrets_client():
    global _secrets
    if _secrets is None:
        _secrets = boto3.client("secretsmanager", config=_BOTO_CFG)
    return _secrets


def stream_name() -> str:
    return os.environ["STREAM_NAME"]


FEEDS = {
    "trip_updates": "https://gtfsapi.translink.ca/v3/gtfsrealtime",
    "vehicle_positions": "https://gtfsapi.translink.ca/v3/gtfsposition",
}

MAX_RECORDS_PER_PUT = 500  # Kinesis PutRecords hard limit
HTTP_TIMEOUT = 15

# One Kinesis shard accepts 1,000 records/sec. Left unpaced, a single poll fires
# ~11,000 records in ~4.5s -- about 2,400/sec -- which throttled hard enough that
# _flush() exhausted its retries, failed the invocation, and EventBridge re-ran
# the whole poll (republishing everything that had already landed).
#
# The function has a 120s timeout and only needs a few seconds of real work, so
# the cheapest fix is to spend some of that budget deliberately rather than pay
# for a second shard. 900/sec leaves headroom under the limit.
TARGET_RECORDS_PER_SEC = 900
_MIN_BATCH_INTERVAL = MAX_RECORDS_PER_PUT / TARGET_RECORDS_PER_SEC

_api_key_cache: str | None = None


def api_key() -> str:
    """Fetch the API key once per cold start, not once per invocation."""
    global _api_key_cache
    if _api_key_cache is None:
        secret = secrets_client().get_secret_value(SecretId=os.environ["SECRET_ID"])["SecretString"]
        _api_key_cache = json.loads(secret)["apikey"]
    return _api_key_cache


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(url, params={"apikey": api_key()}, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(response.content)
    return message


def _opt(obj: Any, field: str) -> Any:
    """Return a protobuf optional field, or None when it was not set."""
    return getattr(obj, field) if obj.HasField(field) else None


def flatten_trip_updates(
    message: gtfs_realtime_pb2.FeedMessage, ingest_ts: int
) -> Iterator[dict[str, Any]]:
    """One row per predicted stop arrival. This is the label source."""
    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue
        update = entity.trip_update
        trip = update.trip
        for stu in update.stop_time_update:
            arrival = stu.arrival if stu.HasField("arrival") else None
            departure = stu.departure if stu.HasField("departure") else None
            yield {
                "record_type": "trip_updates",
                "feed_timestamp": int(message.header.timestamp),
                "ingest_ts": ingest_ts,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "direction_id": _opt(trip, "direction_id"),
                "start_date": trip.start_date or None,
                "schedule_relationship": int(trip.schedule_relationship),
                "vehicle_id": update.vehicle.id or None,
                "stop_id": stu.stop_id or None,
                "stop_sequence": _opt(stu, "stop_sequence"),
                "arrival_time": _opt(arrival, "time") if arrival else None,
                "arrival_delay": _opt(arrival, "delay") if arrival else None,
                "departure_time": _opt(departure, "time") if departure else None,
                "departure_delay": _opt(departure, "delay") if departure else None,
            }


def flatten_vehicle_positions(
    message: gtfs_realtime_pb2.FeedMessage, ingest_ts: int
) -> Iterator[dict[str, Any]]:
    """One row per vehicle sighting."""
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        yield {
            "record_type": "vehicle_positions",
            "feed_timestamp": int(message.header.timestamp),
            "ingest_ts": ingest_ts,
            "vehicle_id": vehicle.vehicle.id or None,
            "trip_id": vehicle.trip.trip_id or None,
            "route_id": vehicle.trip.route_id or None,
            "latitude": float(vehicle.position.latitude),
            "longitude": float(vehicle.position.longitude),
            "bearing": _opt(vehicle.position, "bearing"),
            "speed": _opt(vehicle.position, "speed"),
            "current_stop_sequence": int(vehicle.current_stop_sequence),
            "occupancy_status": _opt(vehicle, "occupancy_status"),
            "vehicle_timestamp": int(vehicle.timestamp),
        }


def partition_key(record: dict[str, Any]) -> str:
    """Partition by route so ordering is preserved per route."""
    return str(record.get("route_id") or record.get("vehicle_id") or "unknown")


def _flush(batch: list[dict[str, Any]], attempts: int = 4) -> int:
    """Send one PutRecords batch, retrying only the individual failures."""
    pending = batch
    for attempt in range(attempts):
        response = kinesis_client().put_records(StreamName=stream_name(), Records=pending)
        if response["FailedRecordCount"] == 0:
            return len(pending)
        pending = [
            rec
            for rec, result in zip(pending, response["Records"], strict=False)
            if "ErrorCode" in result
        ]
        LOG.warning("retrying %d failed records (attempt %d)", len(pending), attempt + 1)
        time.sleep(0.2 * (2**attempt))
    raise RuntimeError(f"{len(pending)} records failed after {attempts} attempts")


def _pace(last_send: float) -> float:
    """Sleep only if the previous batch went out faster than the target rate.

    Returns the timestamp of this send, to be passed back in on the next call.
    """
    if last_send:
        overdue = _MIN_BATCH_INTERVAL - (time.monotonic() - last_send)
        if overdue > 0:
            time.sleep(overdue)
    return time.monotonic()


def publish(records: Iterator[dict[str, Any]]) -> int:
    batch: list[dict[str, Any]] = []
    sent = 0
    last_send = 0.0
    for record in records:
        batch.append(
            {
                "Data": (json.dumps(record) + "\n").encode("utf-8"),
                "PartitionKey": partition_key(record),
            }
        )
        if len(batch) == MAX_RECORDS_PER_PUT:
            last_send = _pace(last_send)
            sent += _flush(batch)
            batch = []
    if batch:
        _pace(last_send)
        sent += _flush(batch)
    return sent


FLATTENERS = {
    "trip_updates": flatten_trip_updates,
    "vehicle_positions": flatten_vehicle_positions,
}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    ingest_ts = int(datetime.now(UTC).timestamp())
    total = 0
    per_feed: dict[str, int] = {}

    for feed_name, url in FEEDS.items():
        message = fetch_feed(url)
        rows = FLATTENERS[feed_name](message, ingest_ts)
        count = publish(rows)
        per_feed[feed_name] = count
        total += count

    # This exact line is what the CloudWatch metric filter parses into the
    # RecordsIngested metric, which the ingest-stalled alarm watches.
    LOG.info(
        json.dumps(
            {
                "metric": "records_ingested",
                "value": total,
                "ts": ingest_ts,
                "per_feed": per_feed,
            }
        )
    )
    return {"records": total, "per_feed": per_feed, "ingest_ts": ingest_ts}
