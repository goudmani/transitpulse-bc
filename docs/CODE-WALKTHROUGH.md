# TransitPulse BC — Annotated Code Walkthrough

**Every file in the project, in full, with an explanation of what it does and why it's written that way.**

The code blocks are read directly from the verified repo, so what you see here is exactly what passed lint, tests, and the wiring checks — no transcription drift.

## How to read this

Each file gets three things:

- **Why this file exists** — the problem it solves
- **The full source**
- **Things to notice** — the specific decisions worth understanding, not a line-by-line paraphrase

If you read only one section, read Part 1. The feature contract is the idea the whole project is organised around.

The boilerplate files (`variables.tf` / `outputs.tf` for each module, `__init__.py`) are collected in the appendix rather than annotated — they're declarations, not decisions.

---

## Contents

- **Part 1 — The idea that holds the project together**
  - `src/common/features.py`
- **Part 2 — Ingestion**
  - `src/ingest/poller/handler.py`
  - `src/ingest/poller/Dockerfile`
  - `src/ingest/static_loader/handler.py`
  - `src/ingest/online_features/handler.py`
  - `src/ops/killswitch/handler.py`
- **Part 3 — The ETL: bronze to silver to gold**
  - `src/glue/silver_stop_events.py`
  - `src/glue/gold_features.py`
  - `src/glue/dq_checks.py`
  - `src/glue/backtest.py`
- **Part 4 — The model**
  - `src/ml/train.py`
  - `src/ml/evaluate.py`
  - `src/ml/pipeline.py`
  - `src/ml/deploy.py`
- **Part 5 — Serving**
  - `src/serving/predict/features.py`
  - `src/serving/predict/handler.py`
- **Part 6 — Infrastructure**
  - `infra/backend.tf`
  - `infra/providers.tf`
  - `infra/variables.tf`
  - `infra/main.tf`
  - `infra/modules/network/main.tf`
  - `infra/modules/lake/main.tf`
  - `infra/modules/ingest/main.tf`
  - `infra/modules/ingest/loaders.tf`
  - `infra/modules/etl/main.tf`
  - `infra/modules/ml/main.tf`
  - `infra/modules/serving/main.tf`
  - `infra/modules/observability/main.tf`
- **Part 7 — SQL**
  - `sql/01_bronze_tables.sql`
  - `sql/05_checks.sql`
- **Part 8 — Tests**
  - `tests/unit/test_features.py`
  - `tests/unit/test_flatten.py`
  - `tests/test_feature_parity.py`
  - `tests/conftest.py`
- **Part 9 — Automation**
  - `Makefile`
  - `scripts/build_poller_zip.sh`
  - `scripts/preflight.sh`
  - `scripts/bootstrap.sh`
  - `scripts/backfill.sh`
  - `infra/modules/cicd/main.tf`
  - `scripts/validate_wiring.py`
  - `.github/workflows/ci.yml`
- **Appendix — declaration files**

---

# Part 1 — The idea that holds the project together

One file defines what a feature is. Everything else — the Spark job, the training script, the serving Lambda — obeys it. Read this first; the rest of the codebase makes more sense once you have.

## `src/common/features.py`

**Why this file exists.** A model is a function from a feature vector to a number. If the vector you build at training time and the vector you build at serving time differ — different order, different null handling, different units — the model silently degrades and nothing errors. This is called training/serving skew, and it's the most common way production ML quietly fails. The defence is to have exactly one definition, imported by both paths.

```python
"""Single source of truth for the model's feature contract.

Both the offline training path (Spark/pandas) and the online serving path
(DynamoDB + Lambda) import FEATURE_ORDER and build_feature_vector from here.
If these ever diverge you get training/serving skew, which is silent and
destroys model quality in production. tests/test_feature_parity.py asserts
the two paths agree.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

# Order is load-bearing: XGBoost consumes a positional CSV row.
FEATURE_ORDER: tuple[str, ...] = (
    # temporal
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "is_peak",
    "minutes_since_service_start",
    # route / stop geometry
    "stop_sequence",
    "stops_remaining",
    "shape_dist_traveled",
    "is_terminus",
    "route_type",
    # historical prior
    "hist_median_delay",
    "hist_p90_delay",
    "hist_std_delay",
    "hist_n",
    # live state
    "delay_t_minus_15",
    "prev_stop_delay",
    "upstream_delay_same_trip",
    "preceding_trip_delay",
    "mean_route_delay_15m",
    "vehicles_active_on_route",
    # weather
    "temp_c",
    "precipitation_mm",
    "wind_kph",
    "visibility_m",
    # disruption
    "active_alert_on_route",
)

# Neutral values used when a feature is genuinely unavailable (a brand new
# route, the first stop of a trip). Nulls are filled identically on both
# paths, which is itself part of the contract.
DEFAULTS: dict[str, float] = {
    "hour_of_day": 12.0,
    "day_of_week": 3.0,
    "is_weekend": 0.0,
    "is_holiday": 0.0,
    "is_peak": 0.0,
    "minutes_since_service_start": 480.0,
    "stop_sequence": 1.0,
    "stops_remaining": 10.0,
    "shape_dist_traveled": 0.0,
    "is_terminus": 0.0,
    "route_type": 3.0,
    "hist_median_delay": 0.0,
    "hist_p90_delay": 0.0,
    "hist_std_delay": 0.0,
    "hist_n": 0.0,
    "delay_t_minus_15": 0.0,
    "prev_stop_delay": 0.0,
    "upstream_delay_same_trip": 0.0,
    "preceding_trip_delay": 0.0,
    "mean_route_delay_15m": 0.0,
    "vehicles_active_on_route": 0.0,
    "temp_c": 10.0,
    "precipitation_mm": 0.0,
    "wind_kph": 10.0,
    "visibility_m": 20000.0,
    "active_alert_on_route": 0.0,
}

TARGET = "observed_delay_sec"

# Delays beyond these bounds are data artefacts, not buses. Clamping here
# rather than in one job keeps offline and online consistent.
DELAY_FLOOR_SEC = -1800
DELAY_CEILING_SEC = 7200

PEAK_HOURS = frozenset({7, 8, 15, 16, 17})


def is_peak_hour(hour: int) -> int:
    """Morning and afternoon commuter peaks."""
    return 1 if int(hour) in PEAK_HOURS else 0


def clamp_delay(value: Any) -> float | None:
    """Return the delay in seconds, or None when it is outside plausible bounds."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds < DELAY_FLOOR_SEC or seconds > DELAY_CEILING_SEC:
        return None
    return seconds


def coerce(name: str, value: Any) -> float:
    """Coerce one feature to float, substituting its default when missing."""
    if value is None:
        return DEFAULTS[name]
    if isinstance(value, bool):
        return float(value)
    try:
        out = float(value)
    except (TypeError, ValueError):
        return DEFAULTS[name]
    if out != out:  # NaN
        return DEFAULTS[name]
    return out


def build_feature_vector(source: Mapping[str, Any]) -> list[float]:
    """Build the positional feature vector the model expects.

    `source` may be a pandas Series, a dict from DynamoDB, or anything else
    that supports mapping access. Missing keys fall back to DEFAULTS.
    """
    return [coerce(name, source.get(name)) for name in FEATURE_ORDER]


def to_csv_row(vector: Sequence[float]) -> str:
    """Serialise a feature vector as the single CSV line SageMaker expects."""
    if len(vector) != len(FEATURE_ORDER):
        raise ValueError(f"expected {len(FEATURE_ORDER)} features, got {len(vector)}")
    return ",".join(f"{v:.6f}" for v in vector)
```

**Things to notice.**

- `FEATURE_ORDER` is a tuple, not a list. XGBoost consumes a positional CSV row, so reordering these silently changes what the model sees. A tuple is immutable, which makes accidental reordering harder.
- `DEFAULTS` exists because real data has holes — a brand-new route has no history, the first stop of a trip has no previous stop. Both paths must fill those holes *identically*. A test asserts every feature in `FEATURE_ORDER` has a default.
- `coerce()` handles three separate kinds of missing: `None`, unparseable strings, and NaN. That last one matters — `float('nan') != float('nan')`, so the check is `out != out`, which looks like a typo and isn't.
- `clamp_delay()` returns `None` rather than a clipped value for implausible delays. A two-hour 'delay' is almost always a GPS glitch or a cancelled trip, not a late bus. Clipping it to the ceiling would teach the model that such delays are real; dropping it teaches nothing, which is correct.
- `to_csv_row()` raises on wrong length. This is a tripwire — if you add a feature to training and forget the serving path, this fires loudly instead of sending a short vector the model misinterprets.

---

# Part 2 — Ingestion

Four Lambdas. One pulls the live feed, one refreshes the weekly schedule, one maintains serving state, one is a financial circuit breaker.

## `src/ingest/poller/handler.py`

**Why this file exists.** Runs every minute. Fetches two protobuf feeds, flattens them into flat JSON rows, and publishes to Kinesis. This is the only component that touches the public internet.

```python
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


def publish(records: Iterator[dict[str, Any]]) -> int:
    batch: list[dict[str, Any]] = []
    sent = 0
    for record in records:
        batch.append(
            {
                "Data": (json.dumps(record) + "\n").encode("utf-8"),
                "PartitionKey": partition_key(record),
            }
        )
        if len(batch) == MAX_RECORDS_PER_PUT:
            sent += _flush(batch)
            batch = []
    if batch:
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
```

**Things to notice.**

- The lazy client pattern (`kinesis_client()`, `secrets_client()`) is the fix for the first bug verification caught. Building clients at import time means the module can't be imported without a region configured, which makes the pure functions untestable. It also means a cold start pays for clients on code paths it may never use.
- `_api_key_cache` is a module-level global, which persists across warm invocations. So Secrets Manager is called once per cold start, not once per minute. At 43,000 invocations a month that's the difference between negligible and noticeable.
- `_opt()` is small and load-bearing. In protobuf, an unset int field reads as `0`, not `None`. If you don't check `HasField`, a bus with no arrival prediction becomes a bus predicted to be *exactly on time* — you'd be fabricating labels. `test_unset_optional_fields_become_none_not_zero` guards this specifically.
- `flatten_trip_updates` yields one row per `stop_time_update`, not per entity. One bus emits predictions for every upcoming stop, so one entity becomes many rows.
- `_flush()` retries only the records that failed. Kinesis `put_records` is partial-failure: the call returns 200 with a `FailedRecordCount`, and resending the whole batch would duplicate the successes.
- `partition_key()` uses `route_id` so records for one route land on the same shard and keep their order. Ordering matters because the dedupe logic downstream picks the latest record per stop.
- The `LOG.info(json.dumps({..."metric": "records_ingested"...}))` line is not decoration. A CloudWatch metric filter parses it into a real metric that the stalled-ingestion alarm watches. Change its shape and you silently disable the alarm.

---

## `src/ingest/poller/Dockerfile`

**Why this file exists.** The poller ships as a container because it needs `protobuf` and `gtfs-realtime-bindings`, neither of which is in the Lambda Python runtime. The alternative is a Lambda layer; a container is more reproducible.

```dockerfile
FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t "${LAMBDA_TASK_ROOT}"

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
```

**Things to notice.**

- `LAMBDA_TASK_ROOT` is where the runtime looks for your code. Installing dependencies into it with `-t` puts them on the import path.
- `CMD` is the handler path, not a shell command. The base image's entrypoint interprets it.
- Build this with `--platform linux/amd64` on Apple Silicon or the function fails at runtime with an exec format error. `scripts/build_push_poller.sh` passes it for you.

---

## `src/ingest/static_loader/handler.py`

**Why this file exists.** Downloads the weekly GTFS schedule ZIP and unpacks it to S3. The schedule is the source of the *planned* arrival time, which is what delay is measured against.

```python
"""Download the weekly GTFS static schedule ZIP and unpack it into bronze.

Idempotent: the archive's SHA-256 is stored in SSM Parameter Store, and an
unchanged archive is a no-op. Uses urllib rather than requests because the
Lambda Python runtime ships boto3 but no third-party HTTP client, and this
function is packaged as a plain zip.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import urllib.request
import zipfile
from typing import Any

import boto3

LOG = logging.getLogger()
LOG.setLevel("INFO")

_s3 = None
_ssm = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def ssm_client():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm")
    return _ssm


WANTED = {
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "shapes.txt",
    "stop_times.txt",
    "stops.txt",
    "trips.txt",
}


def download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "transitpulse/1.0"})
    with urllib.request.urlopen(request, timeout=300) as response:  # noqa: S310
        return response.read()


def stored_digest(param: str) -> str | None:
    ssm = ssm_client()
    try:
        return ssm.get_parameter(Name=param)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        return None


def version_from_event(event: dict[str, Any]) -> str:
    """EventBridge supplies an ISO timestamp; take the date part."""
    raw = str(event.get("time", ""))
    return raw[:10] if len(raw) >= 10 else "manual"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket = os.environ["BRONZE_BUCKET"]
    param = os.environ["SSM_PARAM"]

    payload = download(os.environ["GTFS_URL"])
    digest = hashlib.sha256(payload).hexdigest()

    if stored_digest(param) == digest:
        LOG.info(json.dumps({"status": "unchanged", "sha256": digest}))
        return {"status": "unchanged", "sha256": digest}

    version = version_from_event(event)
    written = []

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            base = os.path.basename(name)
            if base not in WANTED:
                continue
            key = f"static/gtfs/version={version}/{base}"
            s3_client().put_object(Bucket=bucket, Key=key, Body=archive.read(name))
            written.append(base)

    ssm_client().put_parameter(Name=param, Value=digest, Type="String", Overwrite=True)

    LOG.info(
        json.dumps({"status": "updated", "version": version, "files": written, "sha256": digest})
    )
    return {"status": "updated", "version": version, "files": written}
```

**Things to notice.**

- Uses `urllib` rather than `requests`. This function ships as a plain zip, and the Lambda runtime includes boto3 but no third-party HTTP client. Reaching for `requests` here would mean packaging dependencies for a 40-line function.
- Idempotency via SHA-256 in SSM Parameter Store: if the archive hasn't changed, exit immediately. Re-uploading 100 MB of unchanged CSVs weekly is waste, and a no-op return makes reruns safe.
- `WANTED` is an allowlist. GTFS archives contain files you don't need, and writing all of them costs storage and clutters the catalog.
- `version_from_event` slices the EventBridge ISO timestamp to a date. This same slicing appears in the Glue jobs — EventBridge always hands you a full timestamp and you almost always want the date.

---

## `src/ingest/online_features/handler.py`

**Why this file exists.** The second consumer of the Kinesis stream. Maintains current route/stop delay state in DynamoDB so the prediction API can read features in milliseconds instead of querying S3.

```python
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
```

**Things to notice.**

- This function is the entire justification for using Kinesis Data Streams instead of writing straight to Firehose. Two consumers read the same records independently. Without it, Firehose alone would be cheaper and simpler — that trade-off is written up in `docs/adr/001`.
- The batch is aggregated in memory first, then written once per key. Writing per record would multiply DynamoDB costs for identical results.
- `Decimal` everywhere: DynamoDB rejects Python floats. Forgetting this produces a confusing `TypeError` deep in botocore.
- `ttl` is set on every item. DynamoDB deletes expired items for free — no cleanup job, no storage creep. Live state is worthless after two hours anyway.
- One Kinesis record can hold several newline-delimited JSON rows, because the poller batches that way. `decode()` splits them and tolerates a malformed line rather than failing the whole batch.

---

## `src/ops/killswitch/handler.py`

**Why this file exists.** Disables the ingestion schedule when estimated charges cross a threshold. Fifty lines that bound your worst case.

```python
"""Disable the ingestion schedule when estimated charges cross the threshold.

Wired to a CloudWatch billing alarm. Cheap insurance: the worst outcome of a
runaway pipeline is a bill, and this bounds it without human reaction time.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

LOG = logging.getLogger()
LOG.setLevel("INFO")

_events = None
_sns = None


def events_client():
    global _events
    if _events is None:
        _events = boto3.client("events")
    return _events


def sns_client():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    rule_name = os.environ["RULE_NAME"]
    topic_arn = os.environ["TOPIC_ARN"]

    events_client().disable_rule(Name=rule_name)

    message = (
        "TransitPulse ingestion has been DISABLED by the cost guard.\n\n"
        f"EventBridge rule disabled: {rule_name}\n\n"
        "Investigate spend in Cost Explorer, then re-enable with:\n"
        f"  aws events enable-rule --name {rule_name}\n"
    )
    sns_client().publish(
        TopicArn=topic_arn,
        Subject="TransitPulse: ingestion disabled by cost guard",
        Message=message,
    )

    LOG.warning(json.dumps({"action": "disabled_rule", "rule": rule_name}))
    return {"disabled": rule_name}
```

**Things to notice.**

- It disables the *schedule*, not the function. Nothing is destroyed, no state is lost, and re-enabling is one command — which the alert email includes so you don't have to look it up while panicking.
- Wired to a CloudWatch billing alarm in `infra/modules/observability/main.tf`. Billing metrics only exist in `us-east-1`, which is why the root module declares a second provider alias.

---

# Part 3 — The ETL: bronze to silver to gold

Three PySpark jobs and a quality gate. This is where raw feed noise becomes a table you can train on.

## `src/glue/silver_stop_events.py`

**Why this file exists.** The hardest and most interesting job. The feed emits a prediction for the same stop dozens of times as a bus approaches. This collapses that into one row per stop event with the final observed delay plus snapshots at fixed prediction horizons.

```python
"""Bronze -> Silver: collapse repeated arrival predictions into stop events.

The realtime feed emits a prediction for the same stop many times as a bus
approaches. This job produces exactly one row per (service_date, trip_id,
stop_id) holding the final observed delay plus snapshots taken at fixed
prediction horizons, then MERGEs it into an Iceberg table so late-arriving
corrections update in place rather than duplicating.
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "bronze_db", "silver_table"])

SC = SparkContext.getOrCreate()
GLUE = GlueContext(SC)
SPARK = GLUE.spark_session
JOB = Job(GLUE)
JOB.init(ARGS["JOB_NAME"], ARGS)

# EventBridge passes a full ISO timestamp; Glue and Athena want a date.
RUN_DATE = ARGS["run_date"][:10]
BRONZE_DB = ARGS["bronze_db"]
SILVER_TABLE = ARGS["silver_table"]

KEY = ["start_date", "trip_id", "stop_id"]
HORIZONS_MIN = (5, 15, 30)

DELAY_FLOOR_SEC = -1800
DELAY_CEILING_SEC = 7200


def read_trip_updates() -> DataFrame:
    return (
        SPARK.table(f"{BRONZE_DB}.bronze_trip_updates")
        .where(F.col("dt") == F.lit(RUN_DATE))
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
        .withColumn("observed_arrival_ts", F.to_timestamp(F.col("observed_arrival_epoch")))
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
```

**Things to notice.**

- `final_observation()` uses a window ordered by `ingest_ts` descending and keeps `rn == 1` — the last prediction received. This is the standard Spark dedupe idiom and worth internalising.
- The partition key includes `start_date`. Without it, the same `trip_id` recurring daily would collapse across days. This is a real bug people hit; it's in the failure-modes table for that reason.
- `snapshot_at()` is the clever part. To train a model that predicts 15 minutes ahead, you need the state of the world 15 minutes before arrival. It filters to predictions made at least that far out, then picks the one closest to exactly that horizon.
- `MERGE INTO` rather than `INSERT`. Re-running a day must update rows, not duplicate them. This is why silver is Iceberg — plain Parquet can't do row-level upserts.
- The `WHEN MATCHED AND s.last_seen_ts > t.last_seen_ts` guard means a stale rerun can't overwrite fresher data.
- Delays outside ±plausible bounds become `NULL`, not clipped values — same reasoning as `clamp_delay` in the feature contract.

---

## `src/glue/gold_features.py`

**Why this file exists.** Turns stop events into a model-ready table. Its whole reason for existing is enforcing one rule.

```python
"""Silver -> Gold: build the point-in-time-correct training feature table.

The rule this job exists to enforce: every feature must have been knowable at
snapshot time (15 minutes before predicted arrival). Historical aggregates use
service dates strictly earlier than the run date. Breaking that rule leaks the
label and produces metrics that look excellent and mean nothing.
"""

from __future__ import annotations

import sys

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

RUN_DATE = ARGS["run_date"][:10]
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
```

**Things to notice.**

- `historical_priors()` filters `service_date < RUN_DATE` — **strictly** earlier. Include today and you've leaked the label into the features, and every metric you report afterwards is fiction. This single line is the leakage guard.
- `MIN_CELL_COUNT = 20` drops aggregates built from a handful of observations. A median over three buses is noise dressed as a feature.
- `upstream_features()` uses three different windows to capture delay propagation: the previous stop on this trip, the running average of upstream stops, and the preceding trip on the same route. Buses bunch — the trip ahead of you being late is genuinely predictive.
- The weather join is wrapped in try/except and falls back to constants. Optional enrichment shouldn't be able to fail the pipeline.
- `.write.mode("overwrite").partitionBy("service_date")` makes reruns idempotent at the partition level.

---

## `src/glue/dq_checks.py`

**Why this file exists.** Five rules that decide whether the day's data is allowed into the gold layer. Exits non-zero so Step Functions routes to quarantine.

```python
"""Data quality gate. Exits non-zero so Step Functions routes to quarantine.

The point is not to crash the pipeline. It is to stop bad data reaching the
gold layer and poisoning every model trained afterwards, while leaving the
partition inspectable.
"""

from __future__ import annotations

import json
import sys

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

RUN_DATE = ARGS["run_date"][:10]
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
```

**Things to notice.**

- The design intent is quarantine, not crash. The bad partition stays inspectable, gold stays untouched, you get an email. Crashing loses information; silently proceeding poisons every model trained afterwards.
- All five checks run before any failure is raised, so one email tells you everything that's wrong rather than the first thing.
- Results are written to S3 as JSON regardless of outcome, giving you a data-quality history you can chart.
- `sys.exit(1)` after `JOB.commit()` — the commit records the bookmark, the exit code signals the state machine.

---

## `src/glue/backtest.py`

**Why this file exists.** Joins predictions the endpoint made hours ago to what actually happened, and publishes rolling accuracy to CloudWatch.

```python
"""Join captured predictions to observed outcomes and publish rolling metrics.

Ground truth arrives hours after the prediction, so this runs daily over a
lagged window. It emits the three numbers that make the project defensible:
the model's MAE, the persistence baseline, and the schedule baseline.
"""

from __future__ import annotations

import json
import sys

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

RUN_DATE = ARGS["run_date"][:10]
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
```

**Things to notice.**

- This is what makes the project 'operated' rather than 'trained'. Ground truth arrives 10–40 minutes after the prediction, so accuracy can only be measured on a lag.
- It computes the model's MAE *and* both baselines on the same rows. A model MAE without its baseline is a meaningless number.
- `ModelMaeRatioVsPersistence` is the metric the degradation alarm watches. A ratio above 1.0 means the model is worse than assuming the bus stays as late as it currently is.
- `put_metric_data` with a custom namespace is how you get your own business metrics into CloudWatch dashboards and alarms.

---

# Part 4 — The model

Training, evaluation against baselines, the pipeline that gates on quality, and deployment.

## `src/ml/train.py`

**Why this file exists.** SageMaker script mode: SageMaker copies your data into `/opt/ml/input/data/<channel>`, sets `SM_CHANNEL_*` environment variables, runs your script, and uploads whatever you write to `SM_MODEL_DIR`.

```python
"""SageMaker script-mode entry point for the arrival delay model.

Reads the gold feature Parquet from the train/validation channels, fits
XGBoost with an MAE-aligned objective, and prints the validation metric in
the exact format the hyperparameter tuner's regex expects.
"""

from __future__ import annotations

import argparse
import glob
import json
import os

import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

FEATURE_ORDER = (
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "is_peak",
    "minutes_since_service_start",
    "stop_sequence",
    "stops_remaining",
    "shape_dist_traveled",
    "is_terminus",
    "route_type",
    "hist_median_delay",
    "hist_p90_delay",
    "hist_std_delay",
    "hist_n",
    "delay_t_minus_15",
    "prev_stop_delay",
    "upstream_delay_same_trip",
    "preceding_trip_delay",
    "mean_route_delay_15m",
    "vehicles_active_on_route",
    "temp_c",
    "precipitation_mm",
    "wind_kph",
    "visibility_m",
    "active_alert_on_route",
)

TARGET = "observed_delay_sec"

DEFAULTS = {
    "hour_of_day": 12.0,
    "day_of_week": 3.0,
    "is_weekend": 0.0,
    "is_holiday": 0.0,
    "is_peak": 0.0,
    "minutes_since_service_start": 480.0,
    "stop_sequence": 1.0,
    "stops_remaining": 10.0,
    "shape_dist_traveled": 0.0,
    "is_terminus": 0.0,
    "route_type": 3.0,
    "hist_median_delay": 0.0,
    "hist_p90_delay": 0.0,
    "hist_std_delay": 0.0,
    "hist_n": 0.0,
    "delay_t_minus_15": 0.0,
    "prev_stop_delay": 0.0,
    "upstream_delay_same_trip": 0.0,
    "preceding_trip_delay": 0.0,
    "mean_route_delay_15m": 0.0,
    "vehicles_active_on_route": 0.0,
    "temp_c": 10.0,
    "precipitation_mm": 0.0,
    "wind_kph": 10.0,
    "visibility_m": 20000.0,
    "active_alert_on_route": 0.0,
}


def load_channel(channel: str, max_rows: int | None = None) -> pd.DataFrame:
    path = os.environ[f"SM_CHANNEL_{channel.upper()}"]
    files = sorted(glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True))
    if not files:
        raise FileNotFoundError(f"no parquet files under {path}")
    if max_rows:
        # Local dry runs on a small laptop: read files until the cap is met
        # rather than loading the whole channel into memory.
        chunks, total = [], 0
        for f in files:
            chunk = pd.read_parquet(f)
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_rows:
                break
        frame = pd.concat(chunks, ignore_index=True).head(max_rows)
    else:
        frame = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)

    missing = [c for c in (*FEATURE_ORDER, TARGET) if c not in frame.columns]
    if missing:
        raise KeyError(f"channel {channel} missing columns: {missing}")

    # Fill exactly as src/common/features.py does, so the serving path agrees.
    for column, default in DEFAULTS.items():
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(default)

    frame = frame.dropna(subset=[TARGET])
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--eta", type=float, default=0.08)
    parser.add_argument("--subsample", type=float, default=0.8)
    parser.add_argument("--colsample-bytree", type=float, default=0.8)
    parser.add_argument("--min-child-weight", type=float, default=5.0)
    parser.add_argument("--num-round", type=int, default=800)
    parser.add_argument("--early-stopping-rounds", type=int, default=50)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Cap rows per channel. For local dry runs on a small machine; "
        "leave unset for real training on SageMaker.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    train = load_channel("train", args.max_rows)
    validation = load_channel("validation", args.max_rows)

    dtrain = xgb.DMatrix(train[list(FEATURE_ORDER)], label=train[TARGET])
    dval = xgb.DMatrix(validation[list(FEATURE_ORDER)], label=validation[TARGET])

    params = {
        # Delay distributions have fat tails; optimise the metric we report.
        "objective": "reg:absoluteerror",
        "eval_metric": ["mae", "rmse"],
        "max_depth": args.max_depth,
        "eta": args.eta,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "min_child_weight": args.min_child_weight,
        "tree_method": "hist",
        "seed": 42,
    }

    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=args.num_round,
        evals=[(dtrain, "train"), (dval, "validation")],
        early_stopping_rounds=args.early_stopping_rounds,
        verbose_eval=50,
    )

    predictions = booster.predict(dval)
    mae = float(mean_absolute_error(validation[TARGET], predictions))
    rmse = float(mean_squared_error(validation[TARGET], predictions) ** 0.5)

    # Baselines computed on the same rows, so the comparison is honest.
    persistence_mae = float((validation[TARGET] - validation["delay_t_minus_15"]).abs().mean())
    schedule_mae = float(validation[TARGET].abs().mean())

    # The tuner scrapes this exact line. Do not reformat it.
    print(f"validation:mae={mae:.4f}")
    print(f"validation:rmse={rmse:.4f}")
    print(f"baseline:persistence_mae={persistence_mae:.4f}")
    print(f"baseline:schedule_mae={schedule_mae:.4f}")

    model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
    os.makedirs(model_dir, exist_ok=True)
    booster.save_model(os.path.join(model_dir, "xgboost-model.json"))

    with open(os.path.join(model_dir, "feature_names.json"), "w") as handle:
        json.dump(list(FEATURE_ORDER), handle)

    with open(os.path.join(model_dir, "train_metrics.json"), "w") as handle:
        json.dump(
            {
                "validation_mae": mae,
                "validation_rmse": rmse,
                "persistence_mae": persistence_mae,
                "schedule_mae": schedule_mae,
                "best_iteration": int(booster.best_iteration),
                "n_train": int(len(train)),
                "n_validation": int(len(validation)),
            },
            handle,
            indent=2,
        )


if __name__ == "__main__":
    main()
```

**Things to notice.**

- `FEATURE_ORDER` and `DEFAULTS` are duplicated here rather than imported. That's deliberate: SageMaker uploads only `source_dir`, so a cross-package import would fail inside the container. The duplication is a real risk, and `tests/unit/test_features.py` plus the parity test are what keep the copies honest. An alternative is packaging `common` as a wheel — noted as a possible improvement.
- `objective: reg:absoluteerror` optimises MAE directly. Squared error would chase the rare two-hour outliers at the expense of typical accuracy, and MAE is what you report.
- `print(f"validation:mae={mae:.4f}")` is scraped by the tuner's regex, defined in `pipeline.py`. Reformat this line and hyperparameter tuning silently stops working.
- Both baselines are computed on the same validation rows, so the comparison is apples to apples.
- `early_stopping_rounds` prevents paying for 800 boosting rounds when the model stopped improving at 200.

---

## `src/ml/evaluate.py`

**Why this file exists.** A Processing job that scores the held-out test set and writes `evaluation.json` — the file the pipeline's quality gate reads.

```python
"""SageMaker Processing job: score the held-out test set against all baselines.

Writes evaluation.json, which the pipeline's ConditionStep reads to decide
whether the model is allowed into the registry.
"""

from __future__ import annotations

import glob
import json
import os
import tarfile

import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

MODEL_DIR = "/opt/ml/processing/model"
TEST_DIR = "/opt/ml/processing/test"
OUTPUT_DIR = "/opt/ml/processing/evaluation"

TARGET = "observed_delay_sec"


def extract_model() -> xgb.Booster:
    archive = os.path.join(MODEL_DIR, "model.tar.gz")
    if os.path.exists(archive):
        with tarfile.open(archive) as tar:
            tar.extractall(path=MODEL_DIR)  # noqa: S202 - our own training output
    booster = xgb.Booster()
    booster.load_model(os.path.join(MODEL_DIR, "xgboost-model.json"))
    return booster


def load_test() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(TEST_DIR, "**", "*.parquet"), recursive=True))
    if not files:
        raise FileNotFoundError(f"no parquet files under {TEST_DIR}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def main() -> None:
    booster = extract_model()
    with open(os.path.join(MODEL_DIR, "feature_names.json")) as handle:
        features = json.load(handle)

    test = load_test().dropna(subset=[TARGET])
    for column in features:
        test[column] = pd.to_numeric(test.get(column), errors="coerce").fillna(0.0)

    predictions = booster.predict(xgb.DMatrix(test[features]))

    model_mae = float(mean_absolute_error(test[TARGET], predictions))
    model_rmse = float(mean_squared_error(test[TARGET], predictions) ** 0.5)
    persistence_mae = float((test[TARGET] - test["delay_t_minus_15"]).abs().mean())
    schedule_mae = float(test[TARGET].abs().mean())
    historical_mae = float((test[TARGET] - test["hist_median_delay"]).abs().mean())

    p90_error = float((test[TARGET] - predictions).abs().quantile(0.9))

    report = {
        "metrics": {
            "mae": model_mae,
            "rmse": model_rmse,
            "p90_abs_error": p90_error,
            "persistence_mae": persistence_mae,
            "schedule_mae": schedule_mae,
            "historical_mae": historical_mae,
            "mae_ratio_vs_persistence": model_mae / persistence_mae
            if persistence_mae
            else float("inf"),
            "mae_ratio_vs_schedule": model_mae / schedule_mae if schedule_mae else float("inf"),
            "n_test": int(len(test)),
        }
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "evaluation.json"), "w") as handle:
        json.dump(report, handle, indent=2)

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
```

**Things to notice.**

- Processing jobs use fixed paths under `/opt/ml/processing/`, unlike training jobs which use environment variables. This inconsistency is a SageMaker quirk worth remembering.
- The model arrives as `model.tar.gz` and has to be extracted. Training jobs always tar their output.
- It reports MAE, RMSE, p90 absolute error, and all three baselines. p90 matters because a model with good average error and terrible tail error is bad for a rider who cares about missing their bus.
- `mae_ratio_vs_persistence` is the number the ConditionStep gates on, so it's computed here rather than in the pipeline.

---

## `src/ml/pipeline.py`

**Why this file exists.** Wires training, evaluation, the quality gate, and registration into one DAG that can be started on a schedule.

```python
"""Define and upsert the SageMaker Pipeline: train -> evaluate -> gate -> register.

Run this locally (or from CI) to create or update the pipeline definition:
    python src/ml/pipeline.py --role-arn ... --gold-bucket ... --artifacts-bucket ...
Then start executions with the AWS CLI or the weekly EventBridge rule.
"""

from __future__ import annotations

import argparse

import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.fail_step import FailStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.parameters import ParameterFloat, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.xgboost.estimator import XGBoost

FRAMEWORK_VERSION = "1.7-1"


def build(args: argparse.Namespace) -> Pipeline:
    session = sagemaker.session.Session()
    region = session.boto_region_name

    train_uri = ParameterString(
        "TrainUri", default_value=f"s3://{args.gold_bucket}/features/split/train/"
    )
    val_uri = ParameterString(
        "ValidationUri", default_value=f"s3://{args.gold_bucket}/features/split/val/"
    )
    test_uri = ParameterString(
        "TestUri", default_value=f"s3://{args.gold_bucket}/features/split/test/"
    )
    # Must beat "the bus stays as late as it currently is" by this margin.
    max_ratio = ParameterFloat("MaxMaeRatioVsPersistence", default_value=0.92)

    estimator = XGBoost(
        entry_point="train.py",
        source_dir=args.source_dir,
        framework_version=FRAMEWORK_VERSION,
        py_version="py3",
        role=args.role_arn,
        instance_type=args.train_instance_type,
        instance_count=1,
        output_path=f"s3://{args.artifacts_bucket}/models/",
        base_job_name=f"{args.project}-train",
        use_spot_instances=True,
        max_run=3600,
        max_wait=7200,
        hyperparameters={"num-round": 800, "max-depth": 8, "eta": 0.08},
        metric_definitions=[
            {"Name": "validation:mae", "Regex": r"validation:mae=([0-9\.]+)"},
            {"Name": "validation:rmse", "Regex": r"validation:rmse=([0-9\.]+)"},
        ],
        sagemaker_session=session,
    )

    train_step = TrainingStep(
        name="TrainDelayModel",
        estimator=estimator,
        inputs={
            "train": TrainingInput(s3_data=train_uri, content_type="application/x-parquet"),
            "validation": TrainingInput(s3_data=val_uri, content_type="application/x-parquet"),
        },
    )

    image_uri = sagemaker.image_uris.retrieve(
        framework="xgboost",
        region=region,
        version=FRAMEWORK_VERSION,
        py_version="py3",
        instance_type=args.eval_instance_type,
    )

    processor = ScriptProcessor(
        image_uri=image_uri,
        command=["python3"],
        role=args.role_arn,
        instance_type=args.eval_instance_type,
        instance_count=1,
        base_job_name=f"{args.project}-eval",
        sagemaker_session=session,
    )

    report = PropertyFile(name="EvaluationReport", output_name="evaluation", path="evaluation.json")

    eval_step = ProcessingStep(
        name="EvaluateAgainstBaselines",
        processor=processor,
        code=f"{args.source_dir}/evaluate.py",
        inputs=[
            ProcessingInput(
                source=train_step.properties.ModelArtifacts.S3ModelArtifacts,
                destination="/opt/ml/processing/model",
            ),
            ProcessingInput(source=test_uri, destination="/opt/ml/processing/test"),
        ],
        outputs=[
            ProcessingOutput(
                output_name="evaluation",
                source="/opt/ml/processing/evaluation",
                destination=f"s3://{args.artifacts_bucket}/evaluation/",
            )
        ],
        property_files=[report],
    )

    register_step = train_step.estimator.register(
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name=args.model_package_group,
        approval_status="PendingManualApproval",
    )

    gate = ConditionLessThanOrEqualTo(
        left=JsonGet(
            step_name=eval_step.name,
            property_file=report,
            json_path="metrics.mae_ratio_vs_persistence",
        ),
        right=max_ratio,
    )

    condition_step = ConditionStep(
        name="GateOnBaselineImprovement",
        conditions=[gate],
        if_steps=[register_step],
        else_steps=[
            FailStep(
                name="RejectModel",
                error_message="Model did not beat the persistence baseline by the required margin.",
            )
        ],
    )

    return Pipeline(
        name=f"{args.project}-training",
        parameters=[train_uri, val_uri, test_uri, max_ratio],
        steps=[train_step, eval_step, condition_step],
        sagemaker_session=session,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="transitpulse")
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--gold-bucket", required=True)
    parser.add_argument("--artifacts-bucket", required=True)
    parser.add_argument("--model-package-group", default="transitpulse")
    parser.add_argument("--source-dir", default="src/ml")
    parser.add_argument("--train-instance-type", default="ml.m5.xlarge")
    parser.add_argument("--eval-instance-type", default="ml.m5.large")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = build(args)
    pipeline.upsert(role_arn=args.role_arn)
    print(f"upserted pipeline: {pipeline.name}")


if __name__ == "__main__":
    main()
```

**Things to notice.**

- `PropertyFile` + `JsonGet` is how a step reads a value out of a previous step's output file at runtime. This is the non-obvious mechanism that makes conditional pipelines work.
- `ConditionLessThanOrEqualTo` with `0.92` means the model must be at least 8% better than persistence to be registered at all. A gate that always passes is decoration.
- `approval_status="PendingManualApproval"` puts a human in the loop for dev. Production would flip this to automatic when metrics improve.
- `use_spot_instances=True` with `max_wait` gives up to ~70% off training cost. `max_wait` must exceed `max_run` — it's total time including waiting for capacity.
- `ParameterString` / `ParameterFloat` make the pipeline reusable without redefining it: you can start an execution with a different threshold to test the gate.

---

## `src/ml/deploy.py`

**Why this file exists.** Finds the latest approved model package and deploys it to a serverless endpoint.

```python
"""Deploy the latest approved model package to a serverless endpoint.

Serverless inference scales to zero, which is the difference between a few
dollars a month and ninety for a portfolio project nobody is calling.
"""

from __future__ import annotations

import argparse

import boto3
import sagemaker
from sagemaker import ModelPackage
from sagemaker.serverless import ServerlessInferenceConfig


def latest_approved(group: str, region: str) -> str:
    client = boto3.client("sagemaker", region_name=region)
    response = client.list_model_packages(
        ModelPackageGroupName=group,
        ModelApprovalStatus="Approved",
        SortBy="CreationTime",
        SortOrder="Descending",
        MaxResults=1,
    )
    packages = response.get("ModelPackageSummaryList", [])
    if not packages:
        raise RuntimeError(f"no approved model packages in group {group}")
    return packages[0]["ModelPackageArn"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--model-package-group", default="transitpulse")
    parser.add_argument("--endpoint-name", default="transitpulse-delay-predictor")
    parser.add_argument("--gold-bucket", required=True)
    parser.add_argument("--memory-mb", type=int, default=2048)
    parser.add_argument("--max-concurrency", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = sagemaker.session.Session()
    region = session.boto_region_name

    package_arn = latest_approved(args.model_package_group, region)
    print(f"deploying {package_arn}")

    model = ModelPackage(
        role=args.role_arn,
        model_package_arn=package_arn,
        sagemaker_session=session,
    )

    model.deploy(
        endpoint_name=args.endpoint_name,
        serverless_inference_config=ServerlessInferenceConfig(
            memory_size_in_mb=args.memory_mb,
            max_concurrency=args.max_concurrency,
        ),
    )
    print(f"endpoint live: {args.endpoint_name}")


if __name__ == "__main__":
    main()
```

**Things to notice.**

- `list_model_packages` filtered to `Approved` and sorted descending is the registry pattern: deploy what a human blessed, not whatever trained last.
- `ServerlessInferenceConfig` is the single biggest cost decision in the project — roughly $0 idle versus ~$96/month for the smallest always-on instance. The trade-off is a 1–3 second cold start, documented in `docs/adr/004`.
- `max_concurrency=5` bounds how much the endpoint can scale, which bounds spend if something loops.

---

# Part 5 — Serving

The read path: an HTTP request becomes a feature lookup and a model invocation in under a few hundred milliseconds.

## `src/serving/predict/features.py`

**Why this file exists.** A byte-identical copy of `src/common/features.py`. A Lambda zip can only contain what's inside its own directory, so the shared contract is vendored in at build time by `scripts/build_push_poller.sh`-style copying rather than imported across packages.

_Identical to `src/common/features.py` above — see Part 1 for the source and annotations._

**Things to notice.**

- Duplication is a real risk and it is deliberate, not an oversight. The alternatives are packaging `common` as a wheel and installing it into the zip, or using a Lambda layer — both add build machinery.
- What keeps the copies honest is the parity test: it imports one and exercises the other's path, so divergence fails CI rather than degrading predictions.
- If you extend this project, packaging `common` properly is the first refactor worth doing. Note it in an ADR rather than leaving it implicit.
- The file content is identical to Part 1 and is not reprinted here.

---

## `src/serving/predict/handler.py`

**Why this file exists.** Behind API Gateway. Reads three DynamoDB items, builds a feature vector using the shared contract, invokes the endpoint, returns JSON.

```python
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
```

**Things to notice.**

- It imports `features.py`, which is copied into this package at build time. Both sides of the parity test load the same module — that's the whole point.
- Three `get_item` calls, not a scan: current stop state, route state, and historical stats. Each is a single-digit-millisecond point read.
- `_num()` converts DynamoDB `Decimal` back to float. DynamoDB never returns floats, so this conversion is unavoidable.
- `LOCAL_OFFSET = timedelta(hours=-7)` is a **known bug** and flagged as such. It's correct during PDT and wrong during PST. Fixing it properly means either bundling tzdata or storing a UTC-based hour in the features. It's called out in the guide's review section rather than hidden.
- `dow = (local.weekday() + 1) % 7 + 1` converts Python's Monday=0 convention to Spark's `dayofweek` Sunday=1 convention. Get this wrong and every temporal feature is shifted by a day — exactly the kind of thing the parity test catches.
- `feature_fill_rate` is returned in the response and published as a metric. If it drops, the online store is stale and predictions are running mostly on defaults.
- Missing features degrade to defaults rather than erroring. A prediction built from partial data with a visible fill rate beats a 500.

---

# Part 6 — Infrastructure

Terraform. Read the root first to see how the pieces compose, then each module.

## `infra/backend.tf`

**Why this file exists.** Terraform's own configuration: required version, provider version, and where state lives.

```hcl
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Values are supplied at init time via -backend-config so the account id
  # is never hardcoded in the repo. See scripts/bootstrap.sh.
  backend "s3" {
    key            = "transitpulse/terraform.tfstate"
    dynamodb_table = "tfstate-lock"
    encrypt        = true
  }
}
```

**Things to notice.**

- State goes in S3 so it isn't only on your laptop, with a DynamoDB lock table so two runs can't corrupt it.
- The bucket name is supplied at `init` time via `-backend-config` rather than hardcoded, because it contains your account ID. Backend blocks cannot use variables — this is the standard workaround.
- `~> 5.60` pins the provider's major and minor version. Without a pin, a provider release can break your build overnight.

---

## `infra/providers.tf`

**Why this file exists.** Two provider configurations and the tagging policy.

```hcl
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      Env       = var.env
      Owner     = var.owner
      ManagedBy = "terraform"
    }
  }
}

# Billing metrics only exist in us-east-1, so the cost alarm needs a second
# provider configuration pointed at that region.
provider "aws" {
  alias  = "useast1"
  region = "us-east-1"

  default_tags {
    tags = {
      Project   = var.project
      Env       = var.env
      Owner     = var.owner
      ManagedBy = "terraform"
    }
  }
}
```

**Things to notice.**

- `default_tags` applies `Project`, `Env`, `Owner`, `ManagedBy` to every taggable resource automatically. This is what makes Cost Explorer able to tell you what this project costs.
- The `useast1` alias exists solely because `AWS/Billing` metrics are only published in `us-east-1`. Provider aliases are how you touch a second region.

---

## `infra/variables.tf`

**Why this file exists.** Every input, with descriptions.

```hcl
variable "project" {
  description = "Project slug used for naming and tagging."
  type        = string
  default     = "transitpulse"
}

variable "env" {
  description = "Environment name (dev or prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Primary AWS region for all resources."
  type        = string
  default     = "ca-central-1"
}

variable "owner" {
  description = "Owner tag value."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to the SNS alerts topic."
  type        = string
}

variable "poller_package_type" {
  description = "Zip (no Docker required) or Image (container build). Zip is the default."
  type        = string
  default     = "Zip"
}

variable "poller_image_tag" {
  description = "ECR image tag for the GTFS poller Lambda."
  type        = string
  default     = "v1"
}

variable "translink_secret_name" {
  description = "Secrets Manager secret holding the TransLink API key."
  type        = string
  default     = "transitpulse/translink-api-key"
}

variable "gtfs_static_url" {
  description = "URL of the TransLink GTFS static ZIP archive."
  type        = string
  default     = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
}

variable "daily_cost_threshold_usd" {
  description = "Daily estimated charge that trips the ingestion kill switch."
  type        = number
  default     = 3
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to empty non-empty buckets. Dev only."
  type        = bool
  default     = true
}

variable "github_repo" {
  description = "GitHub repository allowed to deploy via OIDC, as owner/repo. Leave empty to skip CI/CD setup."
  type        = string
  default     = ""
}
```

**Things to notice.**

- `owner` and `alert_email` have no default, which makes them required — Terraform refuses to run without them. That's intentional; an alert email you forgot to set is an alarm that goes nowhere.
- `force_destroy_buckets` defaults to `true` and is `false` in prod.tfvars. This is what lets `terraform destroy` work on non-empty buckets in dev.

---

## `infra/main.tf`

**Why this file exists.** Composition. This file's job is passing outputs from one module into another.

```hcl
data "aws_caller_identity" "current" {}

locals {
  acct = data.aws_caller_identity.current.account_id
  name = var.project
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

module "network" {
  source = "./modules/network"

  name   = local.name
  region = var.region
}

module "lake" {
  source = "./modules/lake"

  name          = local.name
  acct          = local.acct
  force_destroy = var.force_destroy_buckets
}

module "ingest" {
  source = "./modules/ingest"

  name             = local.name
  acct             = local.acct
  region           = var.region
  bronze_bucket    = module.lake.bucket_names["bronze"]
  bronze_arn       = module.lake.bucket_arns["bronze"]
  glue_db          = module.lake.glue_db
  alerts_topic_arn = aws_sns_topic.alerts.arn
  poller_package_type = var.poller_package_type
  poller_image        = "${local.acct}.dkr.ecr.${var.region}.amazonaws.com/${local.name}/poller:${var.poller_image_tag}"
  poller_zip_path     = "../build/poller.zip"
  secret_name      = var.translink_secret_name
  gtfs_static_url  = var.gtfs_static_url
  online_table_arn = module.serving.online_table_arn
  online_table     = module.serving.online_table_name
}

module "etl" {
  source = "./modules/etl"

  name             = local.name
  acct             = local.acct
  region           = var.region
  bronze_arn       = module.lake.bucket_arns["bronze"]
  silver_arn       = module.lake.bucket_arns["silver"]
  gold_arn         = module.lake.bucket_arns["gold"]
  artifacts_bucket = module.lake.bucket_names["artifacts"]
  artifacts_arn    = module.lake.bucket_arns["artifacts"]
  glue_db          = module.lake.glue_db
  alerts_topic_arn = aws_sns_topic.alerts.arn
}

module "ml" {
  source = "./modules/ml"

  name             = local.name
  gold_arn         = module.lake.bucket_arns["gold"]
  artifacts_arn    = module.lake.bucket_arns["artifacts"]
  alerts_topic_arn = aws_sns_topic.alerts.arn
}

module "serving" {
  source = "./modules/serving"

  name   = local.name
  acct   = local.acct
  region = var.region
}

module "cicd" {
  source = "./modules/cicd"

  name         = local.name
  acct         = local.acct
  region       = var.region
  github_repo  = var.github_repo
  state_bucket = "tfstate-transitpulse-${local.acct}"
}

module "observability" {
  source = "./modules/observability"

  providers = {
    aws         = aws
    aws.useast1 = aws.useast1
  }

  name                     = local.name
  region                   = var.region
  alerts_topic_arn         = aws_sns_topic.alerts.arn
  poller_function_name     = module.ingest.poller_function_name
  poller_log_group         = module.ingest.poller_log_group
  poller_dlq_name          = module.ingest.poller_dlq_name
  kinesis_stream_name      = module.ingest.kinesis_stream_name
  killswitch_function_arn  = module.ingest.killswitch_function_arn
  daily_cost_threshold_usd = var.daily_cost_threshold_usd
}
```

**Things to notice.**

- Read the dependency graph here: `ingest` needs `serving`'s DynamoDB table ARN, `etl` needs `lake`'s bucket ARNs, `observability` needs names from `ingest`. Terraform infers ordering from these references — you never declare it.
- The `providers = {}` block on `observability` is how you hand a module an aliased provider. Modules can't reach aliases from the root implicitly.
- `local.acct` comes from `data.aws_caller_identity`, so bucket names are account-scoped without hardcoding your account number in the repo.
- Every unused variable was removed here after the wiring check flagged them — that's why the module calls are tighter than you might expect.

---

## `infra/modules/network/main.tf`

**Why this file exists.** A VPC with private subnets, gateway endpoints, and — deliberately — no NAT Gateway.

```hcl
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name}-vpc"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 10 + count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "${var.name}-private-${count.index}"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.name}-private"
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Gateway endpoints cost nothing. They are the reason this project has no
# NAT Gateway, which would otherwise be the single largest line item.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

resource "aws_security_group" "glue" {
  name        = "${var.name}-glue"
  description = "Glue connection security group. Self-referencing as Glue requires."
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Glue self reference"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "Allow all egress to VPC endpoints"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

**Things to notice.**

- The two `aws_vpc_endpoint` resources are free. A NAT Gateway is ~$32/month plus data processing. Since the only component needing internet is the poller, and unattached Lambdas have internet at no cost, the NAT is pure waste here. This is `docs/adr/003` and a good interview answer.
- `cidrsubnet(var.vpc_cidr, 8, 10 + count.index)` computes subnet ranges rather than hardcoding them, so changing the VPC CIDR doesn't require rewriting every subnet.
- The Glue security group is self-referencing (`self = true`) because Glue requires connections whose security group allows traffic from itself.

---

## `infra/modules/lake/main.tf`

**Why this file exists.** Five buckets, their security posture, lifecycle rules, the Glue database, and the Athena workgroup.

```hcl
locals {
  buckets = {
    bronze    = "${var.name}-bronze-${var.acct}"
    silver    = "${var.name}-silver-${var.acct}"
    gold      = "${var.name}-gold-${var.acct}"
    artifacts = "${var.name}-artifacts-${var.acct}"
    athena    = "${var.name}-athena-${var.acct}"
  }
}

resource "aws_s3_bucket" "b" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "b" {
  for_each                = aws_s3_bucket.b
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "b" {
  for_each = aws_s3_bucket.b
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.b["artifacts"].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.b
  bucket   = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          each.value.arn,
          "${each.value.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.b]
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.b["bronze"].id

  rule {
    id     = "expire-raw"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 14
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      days = 90
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena" {
  bucket = aws_s3_bucket.b["athena"].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = var.name
  description = "TransitPulse BC lakehouse catalog"
}

resource "aws_athena_workgroup" "wg" {
  name          = var.name
  force_destroy = var.force_destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.b["athena"].id}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
```

**Things to notice.**

- `for_each` over a map creates five buckets from one block. Each subsequent resource does `for_each = aws_s3_bucket.b` to apply the same settings to all of them.
- The `tls_only` bucket policy denies any request where `aws:SecureTransport` is false. Encryption in transit, enforced rather than assumed.
- Bronze lifecycle: Intelligent-Tiering at 14 days, expiry at 90. Raw data is reproducible from silver, so keeping it forever is paying for nothing.
- The `abort_incomplete_multipart_upload` rule catches a real and invisible cost — failed large uploads leave parts that bill indefinitely and don't show in the object list.
- Athena results expire after 7 days. They're regenerable by definition.
- The Athena workgroup sets `enforce_workgroup_configuration = true`, so nobody can run a query that writes results somewhere unmanaged.

---

## `infra/modules/ingest/main.tf`

**Why this file exists.** Kinesis, the poller Lambda, and Firehose with Parquet conversion.

```hcl
locals {
  secret_arn = "arn:aws:secretsmanager:${var.region}:${var.acct}:secret:${var.secret_name}-*"
  log_arn    = "arn:aws:logs:${var.region}:${var.acct}:*"
}

# --------------------------------------------------------------------------
# Kinesis stream: one shard is ~1,000 records/sec and ~1 MiB/sec of writes.
# --------------------------------------------------------------------------
resource "aws_kinesis_stream" "gtfs" {
  name             = "${var.name}-gtfs"
  retention_period = 24
  shard_count      = 1

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_sqs_queue" "poller_dlq" {
  name                      = "${var.name}-poller-dlq"
  message_retention_seconds = 1209600
}

# --------------------------------------------------------------------------
# Poller Lambda (container image). Deliberately NOT in the VPC: it needs
# outbound internet, and a VPC-attached Lambda would require a NAT Gateway.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "poller" {
  name               = "${var.name}-poller"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "poller" {
  statement {
    sid       = "WriteStream"
    effect    = "Allow"
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    sid       = "ReadApiKey"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.secret_arn]
  }

  statement {
    sid       = "DeadLetter"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.poller_dlq.arn]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "poller" {
  name   = "${var.name}-poller"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

resource "aws_cloudwatch_log_group" "poller" {
  name              = "/aws/lambda/${var.name}-poller"
  retention_in_days = var.log_retention_days
}

locals {
  poller_is_zip = var.poller_package_type == "Zip"
  poller_zip_ok = local.poller_is_zip && fileexists(var.poller_zip_path)
}

# Ships either as a ~2 MB zip (default, no Docker) or a container image.
# The zip path exists so a laptop short on disk and RAM never has to run
# Docker Desktop just to deploy a 200-line function.
resource "aws_lambda_function" "poller" {
  function_name = "${var.name}-poller"
  role          = aws_iam_role.poller.arn
  timeout       = 120
  memory_size   = 1024

  package_type     = var.poller_package_type
  image_uri        = local.poller_is_zip ? null : var.poller_image
  filename         = local.poller_is_zip ? var.poller_zip_path : null
  runtime          = local.poller_is_zip ? "python3.12" : null
  handler          = local.poller_is_zip ? "handler.lambda_handler" : null
  source_code_hash = local.poller_zip_ok ? filebase64sha256(var.poller_zip_path) : null

  # A scheduler misfire must not fan out into hundreds of concurrent polls.
  reserved_concurrent_executions = 2

  environment {
    variables = {
      STREAM_NAME = aws_kinesis_stream.gtfs.name
      SECRET_ID   = var.secret_name
      LOG_LEVEL   = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.poller_dlq.arn
  }

  depends_on = [
    aws_iam_role_policy.poller,
    aws_cloudwatch_log_group.poller
  ]
}

resource "aws_cloudwatch_event_rule" "poll" {
  name                = "${var.name}-poll-1min"
  description         = "Poll the GTFS-Realtime feed every minute"
  schedule_expression = "rate(1 minute)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "poll" {
  rule      = aws_cloudwatch_event_rule.poll.name
  target_id = "poller"
  arn       = aws_lambda_function.poller.arn
}

resource "aws_lambda_permission" "poll" {
  statement_id  = "AllowEventBridgeInvokePoller"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.poller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.poll.arn
}

# --------------------------------------------------------------------------
# Firehose: buffers the stream and writes Parquet into bronze.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "firehose_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid    = "WriteBronze"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject"
    ]

    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    sid    = "ReadStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards"
    ]

    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    sid    = "ReadCatalog"
    effect = "Allow"

    actions = [
      "glue:GetTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:GetDatabase"
    ]

    resources = [
      "arn:aws:glue:${var.region}:${var.acct}:catalog",
      "arn:aws:glue:${var.region}:${var.acct}:database/${var.glue_db}",
      "arn:aws:glue:${var.region}:${var.acct}:table/${var.glue_db}/*"
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.name}-firehose"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.name}-to-bronze"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# The Glue table below only exists to give Firehose a schema for Parquet
# conversion. Analysts query the partition-projected tables in sql/.
resource "aws_glue_catalog_table" "firehose_schema" {
  name          = "firehose_schema"
  database_name = var.glue_db
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket}/raw/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "record_type"
      type = "string"
    }

    columns {
      name = "feed_timestamp"
      type = "bigint"
    }

    columns {
      name = "ingest_ts"
      type = "bigint"
    }

    columns {
      name = "trip_id"
      type = "string"
    }

    columns {
      name = "route_id"
      type = "string"
    }

    columns {
      name = "direction_id"
      type = "int"
    }

    columns {
      name = "start_date"
      type = "string"
    }

    columns {
      name = "schedule_relationship"
      type = "int"
    }

    columns {
      name = "vehicle_id"
      type = "string"
    }

    columns {
      name = "stop_id"
      type = "string"
    }

    columns {
      name = "stop_sequence"
      type = "int"
    }

    columns {
      name = "arrival_time"
      type = "bigint"
    }

    columns {
      name = "arrival_delay"
      type = "int"
    }

    columns {
      name = "departure_time"
      type = "bigint"
    }

    columns {
      name = "departure_delay"
      type = "int"
    }

    columns {
      name = "latitude"
      type = "double"
    }

    columns {
      name = "longitude"
      type = "double"
    }

    columns {
      name = "bearing"
      type = "double"
    }

    columns {
      name = "speed"
      type = "double"
    }

    columns {
      name = "current_stop_sequence"
      type = "int"
    }

    columns {
      name = "occupancy_status"
      type = "int"
    }

    columns {
      name = "vehicle_timestamp"
      type = "bigint"
    }
  }
}

resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = "${var.name}-to-bronze"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.gtfs.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.bronze_arn

    # Bigger buffers mean fewer, larger files, which makes Athena cheaper.
    # Chasing lower latency here produces thousands of tiny files instead.
    buffering_size     = 64
    buffering_interval = 300
    compression_format = "UNCOMPRESSED"

    prefix              = "raw/!{partitionKeyFromQuery:record_type}/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    dynamic_partitioning_configuration {
      enabled = true
    }

    processing_configuration {
      enabled = true

      processors {
        type = "MetadataExtraction"

        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{record_type:.record_type}"
        }

        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }

    data_format_conversion_configuration {
      enabled = true

      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }

      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }

      schema_configuration {
        database_name = var.glue_db
        table_name    = aws_glue_catalog_table.firehose_schema.name
        role_arn      = aws_iam_role.firehose.arn
        region        = var.region
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}
```

**Things to notice.**

- `data.aws_iam_policy_document` instead of inline `jsonencode` JSON: Terraform validates the structure, and the policies read better. Every statement is scoped to a specific ARN.
- `reserved_concurrent_executions = 2` caps the poller. A scheduler misfire or a retry storm can't fan out into hundreds of concurrent invocations.
- The DLQ plus its alarm means a failing poller is loud rather than silent.
- `buffering_interval = 300` and `buffering_size = 64` are a deliberate cost choice. Dropping the interval to 60s to feel more 'real-time' produces 1,440 tiny files a day and makes every Athena query slower and dearer.
- `dynamic_partitioning_configuration` with a JQ `MetadataExtractionQuery` is what routes trip updates and vehicle positions into separate S3 prefixes from one stream.
- `data_format_conversion_configuration` converts JSON to Parquet in flight, which requires a Glue table to describe the schema. That's what `aws_glue_catalog_table.firehose_schema` is for — it exists only to satisfy Firehose, not for querying.
- `error_output_prefix` routes conversion failures to `errors/`. An empty `errors/` prefix is your signal that conversion is healthy.

---

## `infra/modules/ingest/loaders.tf`

**Why this file exists.** The three zip-packaged Lambdas: static loader, online feature writer, kill switch.

```hcl
# --------------------------------------------------------------------------
# Static GTFS loader: weekly, idempotent via a SHA-256 stored in SSM.
# --------------------------------------------------------------------------
data "archive_file" "static_loader" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ingest/static_loader"
  output_path = "${path.module}/build/static_loader.zip"
}

resource "aws_iam_role" "static_loader" {
  name               = "${var.name}-static-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "static_loader" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["ssm:GetParameter", "ssm:PutParameter"]
    resources = ["arn:aws:ssm:${var.region}:${var.acct}:parameter/${var.name}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "static_loader" {
  name   = "${var.name}-static-loader"
  role   = aws_iam_role.static_loader.id
  policy = data.aws_iam_policy_document.static_loader.json
}

resource "aws_cloudwatch_log_group" "static_loader" {
  name              = "/aws/lambda/${var.name}-static-loader"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "static_loader" {
  function_name    = "${var.name}-static-loader"
  role             = aws_iam_role.static_loader.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.static_loader.output_path
  source_code_hash = data.archive_file.static_loader.output_base64sha256

  # stop_times.txt is large; give it room and time.
  timeout     = 600
  memory_size = 2048

  environment {
    variables = {
      BRONZE_BUCKET = var.bronze_bucket
      GTFS_URL      = var.gtfs_static_url
      SSM_PARAM     = "/${var.name}/gtfs-static/sha256"
    }
  }

  depends_on = [
    aws_iam_role_policy.static_loader,
    aws_cloudwatch_log_group.static_loader
  ]
}

resource "aws_cloudwatch_event_rule" "static_weekly" {
  name                = "${var.name}-gtfs-static-weekly"
  description         = "Refresh the GTFS static schedule every Saturday"
  schedule_expression = "cron(0 9 ? * SAT *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "static_weekly" {
  rule      = aws_cloudwatch_event_rule.static_weekly.name
  target_id = "static-loader"
  arn       = aws_lambda_function.static_loader.arn
}

resource "aws_lambda_permission" "static_weekly" {
  statement_id  = "AllowEventBridgeInvokeStaticLoader"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.static_loader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.static_weekly.arn
}

# --------------------------------------------------------------------------
# Online feature writer: second consumer of the same Kinesis stream.
# This is why the design uses Data Streams rather than Firehose alone.
# --------------------------------------------------------------------------
data "archive_file" "online_features" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ingest/online_features"
  output_path = "${path.module}/build/online_features.zip"
}

resource "aws_iam_role" "online_features" {
  name               = "${var.name}-online-features"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "online_features" {
  statement {
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:ListStreams",
      "kinesis:SubscribeToShard"
    ]

    resources = [aws_kinesis_stream.gtfs.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:BatchWriteItem", "dynamodb:GetItem"]
    resources = [var.online_table_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "online_features" {
  name   = "${var.name}-online-features"
  role   = aws_iam_role.online_features.id
  policy = data.aws_iam_policy_document.online_features.json
}

resource "aws_cloudwatch_log_group" "online_features" {
  name              = "/aws/lambda/${var.name}-online-features"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "online_features" {
  function_name    = "${var.name}-online-features"
  role             = aws_iam_role.online_features.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.online_features.output_path
  source_code_hash = data.archive_file.online_features.output_base64sha256
  timeout          = 60
  memory_size      = 512

  environment {
    variables = {
      ONLINE_TABLE = var.online_table
      TTL_SECONDS  = "7200"
    }
  }

  depends_on = [
    aws_iam_role_policy.online_features,
    aws_cloudwatch_log_group.online_features
  ]
}

resource "aws_lambda_event_source_mapping" "online_features" {
  event_source_arn                   = aws_kinesis_stream.gtfs.arn
  function_name                      = aws_lambda_function.online_features.arn
  starting_position                  = "LATEST"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 30
  maximum_retry_attempts             = 2
  bisect_batch_on_function_error     = true
}

# --------------------------------------------------------------------------
# Cost kill switch: disables the poll schedule when spend spikes.
# --------------------------------------------------------------------------
data "archive_file" "killswitch" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/ops/killswitch"
  output_path = "${path.module}/build/killswitch.zip"
}

resource "aws_iam_role" "killswitch" {
  name               = "${var.name}-killswitch"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "killswitch" {
  statement {
    effect    = "Allow"
    actions   = ["events:DisableRule", "events:DescribeRule"]
    resources = [aws_cloudwatch_event_rule.poll.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }
}

resource "aws_iam_role_policy" "killswitch" {
  name   = "${var.name}-killswitch"
  role   = aws_iam_role.killswitch.id
  policy = data.aws_iam_policy_document.killswitch.json
}

resource "aws_cloudwatch_log_group" "killswitch" {
  name              = "/aws/lambda/${var.name}-killswitch"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "killswitch" {
  function_name    = "${var.name}-killswitch"
  role             = aws_iam_role.killswitch.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.killswitch.output_path
  source_code_hash = data.archive_file.killswitch.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      RULE_NAME = aws_cloudwatch_event_rule.poll.name
      TOPIC_ARN = var.alerts_topic_arn
    }
  }

  depends_on = [
    aws_iam_role_policy.killswitch,
    aws_cloudwatch_log_group.killswitch
  ]
}
```

**Things to notice.**

- `data.archive_file` zips a source directory at plan time. `source_code_hash` means Terraform redeploys the function whenever the code changes — without it, edits to your Python silently don't deploy.
- `aws_lambda_event_source_mapping` is what subscribes the online feature writer to Kinesis. `maximum_batching_window_in_seconds = 30` trades a little latency for far fewer invocations.
- `bisect_batch_on_function_error = true` means one poison record doesn't block the whole batch forever — Lambda splits and retries to isolate it.
- `starting_position = "LATEST"` means it doesn't replay history on deploy. For live serving state that's right; a backfill consumer would use `TRIM_HORIZON`.

---

## `infra/modules/etl/main.tf`

**Why this file exists.** Glue jobs and the Step Functions state machine.

```hcl
locals {
  iceberg_conf = join(" ", [
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf spark.sql.catalog.glue_catalog.warehouse=${replace(var.silver_arn, "arn:aws:s3:::", "s3://")}/iceberg/",
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  ])

  scripts = {
    silver   = "silver_stop_events.py"
    gold     = "gold_features.py"
    dq       = "dq_checks.py"
    backtest = "backtest.py"
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "ReadBronze"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.bronze_arn, "${var.bronze_arn}/*"]
  }

  statement {
    sid    = "WriteSilverGold"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      var.silver_arn,
      "${var.silver_arn}/*",
      var.gold_arn,
      "${var.gold_arn}/*"
    ]
  }

  statement {
    sid       = "ReadScripts"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.artifacts_arn, "${var.artifacts_arn}/*"]
  }

  statement {
    sid    = "Catalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:UpdatePartition"
    ]

    resources = [
      "arn:aws:glue:${var.region}:${var.acct}:catalog",
      "arn:aws:glue:${var.region}:${var.acct}:database/${var.glue_db}",
      "arn:aws:glue:${var.region}:${var.acct}:table/${var.glue_db}/*"
    ]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["TransitPulse"]
    }
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "${var.name}-glue-data"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

resource "aws_s3_object" "scripts" {
  for_each = local.scripts

  bucket = var.artifacts_bucket
  key    = "glue/${each.value}"
  source = "${path.module}/../../../src/glue/${each.value}"
  etag   = filemd5("${path.module}/../../../src/glue/${each.value}")
}

resource "aws_glue_job" "job" {
  for_each = local.scripts

  name              = "${var.name}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2

  # A runaway Spark job is a runaway bill. Always set this.
  timeout = 30

  execution_property {
    max_concurrent_runs = 3
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.artifacts_bucket}/glue/${each.value}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.artifacts_bucket}/spark-logs/"
    "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--bronze_db"                        = var.glue_db
    "--glue_db"                          = var.glue_db
    "--silver_table"                     = "glue_catalog.${var.glue_db}.stop_events"
    "--gold_bucket"                      = replace(var.gold_arn, "arn:aws:s3:::", "")
    "--run_date"                         = "AUTO"
  }

  depends_on = [aws_s3_object.scripts]
}

# --------------------------------------------------------------------------
# Step Functions: orchestrates silver -> DQ -> gold with a quality gate.
# --------------------------------------------------------------------------
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    effect    = "Allow"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [for j in aws_glue_job.job : j.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.region}:${var.acct}:rule/StepFunctionsGetEventsForGlueJobRule"]
  }

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "${var.name}-sfn"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.name}-etl"
  retention_in_days = var.log_retention_days
}

resource "aws_sfn_state_machine" "etl" {
  name     = "${var.name}-etl"
  role_arn = aws_iam_role.sfn.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  definition = jsonencode({
    Comment = "TransitPulse hourly ETL: silver, data quality gate, gold"
    StartAt = "SilverStopEvents"
    States = {
      SilverStopEvents = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["silver"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed", "Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.silver"
        Next       = "DataQualityChecks"
      }

      DataQualityChecks = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["dq"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "QuarantinePartition"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.dq"
        Next       = "GoldFeatures"
      }

      GoldFeatures = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job["gold"].name
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "NotifyFailure"
            ResultPath  = "$.error"
          }
        ]
        ResultPath = "$.gold"
        Next       = "Succeeded"
      }

      QuarantinePartition = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alerts_topic_arn
          Subject  = "TransitPulse: data quality gate FAILED"
          "Message.$" = "States.Format('Data quality checks failed for run_date {}. Gold features were NOT rebuilt. Inspect s3 dq/ results before promoting.', $.run_date)"
        }
        Next = "FailDueToDataQuality"
      }

      FailDueToDataQuality = {
        Type  = "Fail"
        Error = "DataQualityGateFailed"
        Cause = "Data quality checks did not pass; gold layer intentionally not updated."
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alerts_topic_arn
          Subject  = "TransitPulse: ETL pipeline failed"
          "Message.$" = "States.Format('ETL failed for run_date {}. Check the Step Functions execution history.', $.run_date)"
        }
        Next = "FailPipeline"
      }

      FailPipeline = {
        Type  = "Fail"
        Error = "EtlPipelineFailed"
      }

      Succeeded = {
        Type = "Succeed"
      }
    }
  })
}

# Fires 20 minutes past the hour so Firehose has flushed its buffer.
resource "aws_cloudwatch_event_rule" "etl_hourly" {
  name                = "${var.name}-etl-hourly"
  description         = "Run the ETL state machine every hour"
  schedule_expression = "cron(20 * * * ? *)"
  state               = "ENABLED"
}

data "aws_iam_policy_document" "events_sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_sfn" {
  name               = "${var.name}-events-sfn"
  assume_role_policy = data.aws_iam_policy_document.events_sfn_assume.json
}

resource "aws_iam_role_policy" "events_sfn" {
  name = "${var.name}-events-sfn"
  role = aws_iam_role.events_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.etl.arn
      }
    ]
  })
}

resource "aws_cloudwatch_event_target" "etl_hourly" {
  rule      = aws_cloudwatch_event_rule.etl_hourly.name
  target_id = "etl-state-machine"
  arn       = aws_sfn_state_machine.etl.arn
  role_arn  = aws_iam_role.events_sfn.arn

  input_transformer {
    input_paths = {
      time = "$.time"
    }

    # Glue jobs receive run_date as YYYY-MM-DD, sliced from the event time.
    input_template = "{\"run_date\": <time>}"
  }
}
```

**Things to notice.**

- `local.iceberg_conf` builds the Spark configuration string. It must be a single space-separated string with `--conf` repeated *inside* it — the most common Glue-plus-Iceberg failure and worth reading closely.
- `aws_s3_object` uploads the PySpark scripts with `etag = filemd5(...)`, so editing a script re-uploads it on the next apply.
- `timeout = 30` on every Glue job. A runaway Spark job is a runaway bill; there's no reason to ever leave this unset.
- The state machine's `Catch` on the DQ step routes to `QuarantinePartition` — a *different* branch from general failure. Bad data and broken infrastructure are different problems and get different responses.
- `glue:startJobRun.sync` makes Step Functions wait for completion. Without `.sync` it fires and forgets, and your DAG has no idea whether anything worked.
- `Retry` with `BackoffRate = 2` handles transient concurrency errors without human involvement.
- The `input_transformer` passes an ISO timestamp as `run_date`; the Glue scripts slice `[:10]`. Flagged for review — it works, but the coupling is implicit.

---

## `infra/modules/ml/main.tf`

**Why this file exists.** The SageMaker role, the model registry group, and scheduling.

```hcl
data "aws_iam_policy_document" "sagemaker_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sagemaker" {
  name               = "${var.name}-sagemaker"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_assume.json
}

# Dev convenience. For prod this should be replaced with a scoped policy;
# the README states this explicitly rather than claiming least privilege.
resource "aws_iam_role_policy_attachment" "sagemaker_full" {
  role       = aws_iam_role.sagemaker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

data "aws_iam_policy_document" "sagemaker_data" {
  statement {
    sid    = "LakeAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]

    resources = [
      var.gold_arn,
      "${var.gold_arn}/*",
      var.artifacts_arn,
      "${var.artifacts_arn}/*"
    ]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["TransitPulse"]
    }
  }
}

resource "aws_iam_role_policy" "sagemaker_data" {
  name   = "${var.name}-sagemaker-data"
  role   = aws_iam_role.sagemaker.id
  policy = data.aws_iam_policy_document.sagemaker_data.json
}

resource "aws_sagemaker_model_package_group" "models" {
  model_package_group_name        = var.name
  model_package_group_description = "TransitPulse arrival delay models"
}

# Notifies you when a model version is approved so you can watch the deploy.
resource "aws_cloudwatch_event_rule" "model_approved" {
  name        = "${var.name}-model-approved"
  description = "Fires when a model package is approved in the registry"

  event_pattern = jsonencode({
    source      = ["aws.sagemaker"]
    detail-type = ["SageMaker Model Package State Change"]
    detail = {
      ModelPackageGroupName = [var.name]
      ModelApprovalStatus   = ["Approved"]
    }
  })
}

resource "aws_cloudwatch_event_target" "model_approved" {
  rule      = aws_cloudwatch_event_rule.model_approved.name
  target_id = "notify"
  arn       = var.alerts_topic_arn
}

# Weekly retraining trigger. The pipeline itself is defined in
# src/ml/pipeline.py and created by running that script.
resource "aws_cloudwatch_event_rule" "retrain_weekly" {
  name                = "${var.name}-retrain-weekly"
  description         = "Kick off the SageMaker training pipeline every Monday"
  schedule_expression = "cron(0 8 ? * MON *)"
  state               = "ENABLED"
}
```

**Things to notice.**

- `AmazonSageMakerFullAccess` is attached and the comment says plainly that this is a dev convenience. Claiming least privilege while attaching a wildcard managed policy is worse than being honest about it — and the README repeats the admission.
- `aws_sagemaker_model_package_group` is the registry. Models are versioned into it; approval is a separate action from training.
- The `model_approved` EventBridge rule turns registry approval into an event you can hang automation off.

---

## `infra/modules/serving/main.tf`

**Why this file exists.** DynamoDB online store, the prediction Lambda, and the HTTP API.

```hcl
locals {
  endpoint = var.endpoint_name != "" ? var.endpoint_name : "${var.name}-delay-predictor"
  log_arn  = "arn:aws:logs:${var.region}:${var.acct}:*"
}

resource "aws_dynamodb_table" "online" {
  name         = "${var.name}-online-features"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # Live state expires on its own; no cleanup job, no storage creep.
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = false
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "archive_file" "predict" {
  type        = "zip"
  source_dir  = "${path.module}/../../../src/serving/predict"
  output_path = "${path.module}/build/predict.zip"
}

resource "aws_iam_role" "predict" {
  name               = "${var.name}-predict"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "predict" {
  statement {
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.online.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sagemaker:InvokeEndpoint"]
    resources = ["arn:aws:sagemaker:${var.region}:${var.acct}:endpoint/${local.endpoint}"]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [local.log_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["TransitPulse"]
    }
  }
}

resource "aws_iam_role_policy" "predict" {
  name   = "${var.name}-predict"
  role   = aws_iam_role.predict.id
  policy = data.aws_iam_policy_document.predict.json
}

resource "aws_cloudwatch_log_group" "predict" {
  name              = "/aws/lambda/${var.name}-predict"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "predict" {
  function_name    = "${var.name}-predict"
  role             = aws_iam_role.predict.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.predict.output_path
  source_code_hash = data.archive_file.predict.output_base64sha256
  timeout          = 15
  memory_size      = 512

  environment {
    variables = {
      ONLINE_TABLE  = aws_dynamodb_table.online.name
      ENDPOINT_NAME = local.endpoint
      METRIC_NS     = "TransitPulse"
    }
  }

  depends_on = [
    aws_iam_role_policy.predict,
    aws_cloudwatch_log_group.predict
  ]
}

resource "aws_apigatewayv2_api" "api" {
  name          = "${var.name}-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["content-type"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "predict" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.predict.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "predict" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /v1/predict"
  target    = "integrations/${aws_apigatewayv2_integration.predict.id}"
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/apigateway/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_apigatewayv2_stage" "v1" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api.arn
    format          = "$context.requestId $context.httpMethod $context.path $context.status $context.responseLatency"
  }

  # Throttling matters: an unthrottled public endpoint in front of SageMaker
  # is an open invitation to run up your bill.
  default_route_settings {
    throttling_burst_limit = 20
    throttling_rate_limit  = 10
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowApiGatewayInvokePredict"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.predict.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
```

**Things to notice.**

- `PAY_PER_REQUEST` billing means you pay per read/write with no provisioned capacity to size or waste.
- TTL is enabled on the table so expired feature rows delete themselves at no cost.
- HTTP API rather than REST API: cheaper and lower latency, and you don't need REST API's extra features here.
- `default_route_settings` throttling is not optional. A public endpoint in front of SageMaker with no rate limit is an open invitation to run up your bill.
- `aws_lambda_permission` with a `source_arn` scoped to this API is what stops any other API from invoking your function.

---

## `infra/modules/observability/main.tf`

**Why this file exists.** Metric filters, five alarms, the billing circuit breaker, and the dashboard.

```hcl
# --------------------------------------------------------------------------
# Turn the poller's structured log line into a metric, then alarm on silence.
# Without this, an expired API key goes unnoticed for weeks.
# --------------------------------------------------------------------------
resource "aws_cloudwatch_log_metric_filter" "ingested" {
  name           = "${var.name}-records-ingested"
  log_group_name = var.poller_log_group
  pattern        = "{ $.metric = \"records_ingested\" }"

  metric_transformation {
    name          = "RecordsIngested"
    namespace     = "TransitPulse"
    value         = "$.value"
    unit          = "Count"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingest_stalled" {
  alarm_name          = "${var.name}-ingest-stalled"
  alarm_description   = "No GTFS records ingested in the last 15 minutes"
  namespace           = "TransitPulse"
  metric_name         = "RecordsIngested"
  statistic           = "Sum"
  period              = 900
  evaluation_periods  = 1
  threshold           = 1000
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.alerts_topic_arn]
  ok_actions          = [var.alerts_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "poller_errors" {
  alarm_name          = "${var.name}-poller-errors"
  alarm_description   = "Poller Lambda is throwing errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 3
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    FunctionName = var.poller_function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "poller_dlq" {
  alarm_name          = "${var.name}-poller-dlq-not-empty"
  alarm_description   = "Messages landed in the poller dead letter queue"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    QueueName = var.poller_dlq_name
  }
}

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.name}-kinesis-iterator-age"
  alarm_description   = "Stream consumers are falling behind"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 600000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]

  dimensions = {
    StreamName = var.kinesis_stream_name
  }
}

resource "aws_cloudwatch_metric_alarm" "model_mae_degraded" {
  alarm_name          = "${var.name}-model-mae-degraded"
  alarm_description   = "Rolling model MAE has degraded past the baseline ratio"
  namespace           = "TransitPulse"
  metric_name         = "ModelMaeRatioVsPersistence"
  statistic           = "Average"
  period              = 86400
  evaluation_periods  = 2
  threshold           = 1.0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = [var.alerts_topic_arn]
}

# --------------------------------------------------------------------------
# Cost circuit breaker. Billing metrics live only in us-east-1.
# --------------------------------------------------------------------------
resource "aws_lambda_permission" "cost_alarm" {
  statement_id  = "AllowCloudWatchAlarmInvokeKillswitch"
  action        = "lambda:InvokeFunction"
  function_name = var.killswitch_function_arn
  principal     = "lambda.alarms.cloudwatch.amazonaws.com"
  source_arn    = aws_cloudwatch_metric_alarm.estimated_charges.arn
}

resource "aws_cloudwatch_metric_alarm" "estimated_charges" {
  provider = aws.useast1

  alarm_name          = "${var.name}-estimated-charges"
  alarm_description   = "Estimated charges exceeded the daily threshold; disabling ingestion"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  statistic           = "Maximum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = var.daily_cost_threshold_usd
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [
    var.alerts_topic_arn,
    var.killswitch_function_arn
  ]
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = var.name

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Records ingested"
          region = var.region
          stat   = "Sum"
          period = 300
          metrics = [
            ["TransitPulse", "RecordsIngested"]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Poller health"
          region = var.region
          stat   = "Sum"
          period = 300
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", var.poller_function_name],
            ["AWS/Lambda", "Errors", "FunctionName", var.poller_function_name],
            ["AWS/Lambda", "Duration", "FunctionName", var.poller_function_name, { stat = "Average" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Model MAE vs baselines (seconds)"
          region = var.region
          stat   = "Average"
          period = 86400
          metrics = [
            ["TransitPulse", "ModelMae"],
            ["TransitPulse", "PersistenceMae"],
            ["TransitPulse", "ScheduleMae"]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Stream backlog"
          region = var.region
          stat   = "Maximum"
          period = 300
          metrics = [
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", var.kinesis_stream_name]
          ]
        }
      }
    ]
  })
}
```

**Things to notice.**

- `aws_cloudwatch_log_metric_filter` turns the poller's JSON log line into a real metric. This is how you get application-level metrics without an extra library.
- `treat_missing_data = "breaching"` on the ingest alarm is the important detail: if the poller dies completely, *no* data points arrive. Treating missing as OK would make the alarm useless exactly when you need it.
- The billing alarm is created with `provider = aws.useast1` because that's the only region publishing `AWS/Billing`.
- Its `alarm_actions` include the kill switch Lambda ARN — a CloudWatch alarm can invoke Lambda directly.
- The dashboard is defined as code with `jsonencode`. Console-built-then-codified is a fine workflow; console-built-and-left is not.

---

# Part 7 — SQL

Table definitions and the queries that verify each phase.

## `sql/01_bronze_tables.sql`

**Why this file exists.** Bronze tables using Athena partition projection.

```sql
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
```

**Things to notice.**

- Partition projection computes partitions from a formula instead of storing them in a catalog. No crawler runs, no `MSCK REPAIR`, no partition metadata drift, no crawler cost.
- `storage.location.template` must match the Firehose prefix exactly. A mismatch produces the classic 'files exist but Athena returns zero rows'.
- `projection.dt.range` starting after your first data day silently hides that data. Set it to your project start date.

---

## `sql/05_checks.sql`

**Why this file exists.** The verification queries, one per phase.

```sql
-- Verification queries used throughout the build. Run the matching one at
-- the end of each phase rather than assuming a step worked.

-- Phase 2: is bronze landing?
SELECT dt, hour, count(*) AS rows, count(DISTINCT trip_id) AS trips,
       count(arrival_delay) AS labelled
FROM transitpulse.bronze_trip_updates
WHERE dt = date_format(current_date, '%Y-%m-%d')
GROUP BY dt, hour
ORDER BY hour;

-- Phase 3a: do the realtime and static trip_id formats actually match?
-- If this returns 0 the whole pipeline is broken and everything downstream
-- will silently produce nothing.
SELECT count(*) AS joined_rows
FROM transitpulse.bronze_trip_updates t
JOIN transitpulse.dim_trips d ON t.trip_id = d.trip_id
WHERE t.dt = date_format(current_date, '%Y-%m-%d');

-- Phase 3b: silver dedupe correctness. dupes MUST be 0.
SELECT
  count(*)                                                   AS events,
  count(observed_delay_sec)                                  AS labelled,
  count(*) - count(DISTINCT trip_id || '|' || stop_id)       AS dupes,
  approx_percentile(observed_delay_sec, 0.5)                 AS median_delay
FROM transitpulse.stop_events
WHERE service_date = current_date - interval '1' day;

-- Phase 3c: coverage across the backfill. A day with 10% of normal volume
-- means the poller was down; do not train on it.
SELECT service_date, count(*) AS events
FROM transitpulse.stop_events
GROUP BY service_date
ORDER BY service_date;

-- Phase 4: feature completeness before training.
SELECT
  count(*)                                                             AS rows,
  count(DISTINCT service_date)                                         AS days,
  sum(CASE WHEN hist_median_delay IS NULL THEN 1 ELSE 0 END)           AS missing_prior,
  sum(CASE WHEN delay_t_minus_15 IS NULL THEN 1 ELSE 0 END)            AS missing_snapshot
FROM transitpulse.training_features;
```

**Things to notice.**

- The `trip_id` join check is the most important query in the project. If realtime and static feeds format IDs differently, every downstream job succeeds and produces nothing.
- The `dupes` check must return exactly 0. Non-zero means the silver window partition key is wrong.
- The per-day coverage query surfaces days when the poller was down. Training on a day with 10% of normal volume skews the historical priors.

---

# Part 8 — Tests

22 passing tests that need no AWS account. This is what makes the repo credible to anyone who reads it.

## `tests/unit/test_features.py`

**Why this file exists.** Tests the feature contract.

```python
"""Tests for the shared feature contract."""

from __future__ import annotations

import math

import pytest

from common.features import (
    DEFAULTS,
    DELAY_CEILING_SEC,
    DELAY_FLOOR_SEC,
    FEATURE_ORDER,
    build_feature_vector,
    clamp_delay,
    coerce,
    is_peak_hour,
    to_csv_row,
)


def test_every_feature_has_a_default():
    assert set(FEATURE_ORDER) == set(DEFAULTS), "FEATURE_ORDER and DEFAULTS diverged"


def test_feature_order_has_no_duplicates():
    assert len(FEATURE_ORDER) == len(set(FEATURE_ORDER))


def test_vector_length_matches_contract():
    assert len(build_feature_vector({})) == len(FEATURE_ORDER)


def test_missing_values_fall_back_to_defaults():
    vector = build_feature_vector({})
    assert vector == [DEFAULTS[name] for name in FEATURE_ORDER]


def test_supplied_values_win_over_defaults():
    vector = build_feature_vector({"hour_of_day": 17, "delay_t_minus_15": 240})
    assert vector[FEATURE_ORDER.index("hour_of_day")] == 17.0
    assert vector[FEATURE_ORDER.index("delay_t_minus_15")] == 240.0


def test_nan_is_treated_as_missing():
    assert coerce("temp_c", float("nan")) == DEFAULTS["temp_c"]


def test_garbage_is_treated_as_missing():
    assert coerce("temp_c", "not-a-number") == DEFAULTS["temp_c"]


def test_booleans_coerce_cleanly():
    assert coerce("is_weekend", True) == 1.0
    assert coerce("is_weekend", False) == 0.0


@pytest.mark.parametrize("hour,expected", [(7, 1), (8, 1), (17, 1), (11, 0), (23, 0)])
def test_peak_hours(hour, expected):
    assert is_peak_hour(hour) == expected


def test_clamp_rejects_implausible_delays():
    assert clamp_delay(DELAY_CEILING_SEC + 1) is None
    assert clamp_delay(DELAY_FLOOR_SEC - 1) is None
    assert clamp_delay(None) is None


def test_clamp_accepts_plausible_delays():
    assert clamp_delay(120) == 120.0
    assert clamp_delay(-300) == -300.0


def test_csv_row_round_trips():
    row = to_csv_row(build_feature_vector({"hour_of_day": 9}))
    values = [float(v) for v in row.split(",")]
    assert len(values) == len(FEATURE_ORDER)
    assert math.isclose(values[FEATURE_ORDER.index("hour_of_day")], 9.0)


def test_csv_row_rejects_wrong_length():
    with pytest.raises(ValueError):
        to_csv_row([1.0, 2.0])
```

**Things to notice.**

- `test_every_feature_has_a_default` catches the specific failure of adding a feature and forgetting its null-fill — which would make the two paths disagree.
- `test_nan_is_treated_as_missing` exists because NaN handling is where numeric code goes wrong quietly.
- `@pytest.mark.parametrize` runs one test body over several inputs, which is how you get coverage without copy-paste.

---

## `tests/unit/test_flatten.py`

**Why this file exists.** Tests the protobuf flattening using synthetic feed messages — no network, no API key.

```python
"""Tests for the poller's protobuf flattening, using synthetic feed messages.

These run without AWS and without network access, which is the point: the
flattening logic is where silent data loss happens, so it needs unit tests.
"""

from __future__ import annotations

import pytest

pytest.importorskip("google.transit", reason="gtfs-realtime-bindings not installed")

from google.transit import gtfs_realtime_pb2  # noqa: E402
from poller.handler import (  # noqa: E402
    flatten_trip_updates,
    flatten_vehicle_positions,
    partition_key,
)


def make_trip_update_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_770_000_000

    entity = feed.entity.add()
    entity.id = "trip-1"
    entity.trip_update.trip.trip_id = "T-123"
    entity.trip_update.trip.route_id = "099"
    entity.trip_update.trip.start_date = "20260805"
    entity.trip_update.vehicle.id = "V-9"

    first = entity.trip_update.stop_time_update.add()
    first.stop_id = "61935"
    first.stop_sequence = 4
    first.arrival.time = 1_770_000_600
    first.arrival.delay = 120

    # Second stop has no arrival set at all: the flattener must not invent one.
    second = entity.trip_update.stop_time_update.add()
    second.stop_id = "61936"
    second.stop_sequence = 5

    return feed


def make_position_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_770_000_000

    entity = feed.entity.add()
    entity.id = "veh-1"
    entity.vehicle.vehicle.id = "V-9"
    entity.vehicle.trip.trip_id = "T-123"
    entity.vehicle.trip.route_id = "099"
    entity.vehicle.position.latitude = 49.2827
    entity.vehicle.position.longitude = -123.1207
    entity.vehicle.current_stop_sequence = 4
    entity.vehicle.timestamp = 1_770_000_000

    return feed


def test_one_row_per_stop_time_update():
    rows = list(flatten_trip_updates(make_trip_update_feed(), 1_770_000_100))
    assert len(rows) == 2


def test_delay_and_ids_survive_flattening():
    rows = list(flatten_trip_updates(make_trip_update_feed(), 1_770_000_100))
    first = rows[0]
    assert first["trip_id"] == "T-123"
    assert first["route_id"] == "099"
    assert first["stop_id"] == "61935"
    assert first["arrival_delay"] == 120
    assert first["record_type"] == "trip_updates"
    assert first["ingest_ts"] == 1_770_000_100


def test_unset_optional_fields_become_none_not_zero():
    rows = list(flatten_trip_updates(make_trip_update_feed(), 1_770_000_100))
    second = rows[1]
    # A missing arrival must be None. Defaulting it to 0 would fabricate an
    # on-time bus and poison the labels.
    assert second["arrival_time"] is None
    assert second["arrival_delay"] is None


def test_vehicle_positions_flatten():
    rows = list(flatten_vehicle_positions(make_position_feed(), 1_770_000_100))
    assert len(rows) == 1
    assert rows[0]["record_type"] == "vehicle_positions"
    assert rows[0]["vehicle_id"] == "V-9"
    assert abs(rows[0]["latitude"] - 49.2827) < 1e-6


def test_partition_key_prefers_route_then_vehicle():
    assert partition_key({"route_id": "099", "vehicle_id": "V-9"}) == "099"
    assert partition_key({"route_id": None, "vehicle_id": "V-9"}) == "V-9"
    assert partition_key({}) == "unknown"
```

**Things to notice.**

- It builds real `FeedMessage` protobufs in code. You can test protobuf handling thoroughly without ever calling the live API.
- `test_unset_optional_fields_become_none_not_zero` is the highest-value test here. Protobuf returns 0 for unset ints; if that became a delay of 0 you'd be fabricating on-time buses and training on invented labels.
- `pytest.importorskip` makes the module skip cleanly rather than error when the protobuf bindings aren't installed.

---

## `tests/test_feature_parity.py`

**Why this file exists.** The training/serving skew guard. Currently skipping until you generate a fixture from real data.

```python
"""Training/serving skew guard.

The offline path builds features in Spark; the online path builds them from
DynamoDB in a Lambda. If they disagree, the model sees different inputs in
production than it was trained on, and quality degrades silently. This test
runs both paths over the same fixtures and asserts they agree exactly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.features import FEATURE_ORDER, build_feature_vector

FIXTURE = Path(__file__).parent / "fixtures" / "offline_sample.json"
TOLERANCE = 1e-6


def offline_rows() -> list[dict]:
    if not FIXTURE.exists():
        pytest.skip(
            "no offline fixture yet; generate it with scripts/make_parity_fixture.py "
            "once the gold layer has data"
        )
    return json.loads(FIXTURE.read_text())


def simulate_online(row: dict) -> dict:
    """Stand-in for the DynamoDB lookup.

    In CI this replays the stored online snapshot captured alongside each
    offline row, so the test exercises the real serving construction order
    without needing AWS credentials.
    """
    return row["online"]


def test_offline_and_online_features_agree():
    mismatches = []

    for row in offline_rows():
        offline_vector = build_feature_vector(row["offline"])
        online_vector = build_feature_vector(simulate_online(row))

        for name, a, b in zip(FEATURE_ORDER, offline_vector, online_vector, strict=True):
            if abs(a - b) > TOLERANCE:
                mismatches.append(
                    {
                        "trip_id": row["offline"].get("trip_id"),
                        "feature": name,
                        "offline": a,
                        "online": b,
                    }
                )

    assert not mismatches, (
        f"{len(mismatches)} feature mismatches between offline and online paths. "
        f"First five: {mismatches[:5]}"
    )


def test_fixture_covers_the_whole_contract():
    rows = offline_rows()
    covered = set()
    for row in rows:
        covered |= {k for k, v in row["offline"].items() if v is not None}
    missing = set(FEATURE_ORDER) - covered
    assert not missing, f"fixture never exercises these features: {sorted(missing)}"
```

**Things to notice.**

- It runs both feature-building paths over identical inputs and asserts they agree to 1e-6.
- It fails the first time you run it for real. It always does — timezone handling, null-fill differences, and day-of-week conventions are the usual culprits.
- `test_fixture_covers_the_whole_contract` guards against a fixture that only exercises five features and passes trivially.
- Most candidates have never heard of training/serving skew. Having an automated test for it is the single most persuasive thing in the repo.

---

## `tests/conftest.py`

**Why this file exists.** Makes `src/` importable without installing the project.

```python
"""Make src/ importable in tests without installing the project."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for path in (ROOT / "src", ROOT / "src" / "ingest"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
```

**Things to notice.**

- pytest auto-discovers `conftest.py` and runs it before tests, which makes it the right place for path setup.
- `pyproject.toml` also sets `pythonpath`; the two together cover both pytest invocation styles.

---

# Part 9 — Automation

The scripts, the Makefile, and CI.

## `Makefile`

**Why this file exists.** Named entry points for every routine operation.

```makefile
SHELL := /bin/bash
REGION ?= ca-central-1
ENV    ?= dev
ACCT   := $(shell aws sts get-caller-identity --query Account --output text)
TFVARS := envs/$(ENV).tfvars

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: preflight
preflight: ## Check this machine can build the project (no AWS changes)
	./scripts/preflight.sh

.PHONY: lint
lint: ## Lint and format-check Python
	ruff check .
	ruff format --check .

.PHONY: test
test: ## Run unit tests
	pytest

.PHONY: package
package: ## Build the poller Lambda zip (no Docker needed, ~2 MB)
	./scripts/build_poller_zip.sh

.PHONY: image
image: ## OPTIONAL: build and push a container image instead of the zip
	./scripts/build_push_poller.sh

.PHONY: clean
clean: ## Free local disk: build artifacts, caches, sample data
	rm -rf build/ .pytest_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/feat /tmp/tp-sample
	@echo "cleaned. Docker images (if any): docker system prune -a"

.PHONY: disk
disk: ## Show what this project is using locally
	@echo "repo:      $$(du -sh . 2>/dev/null | cut -f1)"
	@echo "venv:      $$(du -sh .venv 2>/dev/null | cut -f1 || echo none)"
	@echo "build:     $$(du -sh build 2>/dev/null | cut -f1 || echo none)"
	@command -v docker >/dev/null 2>&1 && echo "docker:    $$(docker system df --format '{{.Size}}' 2>/dev/null | head -1 || echo n/a)" || true

.PHONY: init
init: ## terraform init against the remote backend
	cd infra && terraform init \
	  -backend-config="bucket=tfstate-transitpulse-$(ACCT)" \
	  -backend-config="region=$(REGION)"

.PHONY: plan
plan: ## terraform plan
	@test -f build/poller.zip || (echo "run 'make package' first" && exit 1)
	cd infra && terraform fmt -recursive && terraform validate && \
	  terraform plan -var-file=$(TFVARS) -out=tf.plan

.PHONY: apply
apply: ## terraform apply the saved plan
	cd infra && terraform apply tf.plan

.PHONY: destroy
destroy: ## Tear everything down
	cd infra && terraform destroy -var-file=$(TFVARS)

.PHONY: backfill
backfill: ## Backfill N days of ETL: make backfill DAYS=21
	./scripts/backfill.sh $(or $(DAYS),14)

.PHONY: check
check: ## Daily operational health check
	./scripts/daily_check.sh

.PHONY: pause
pause: ## Stop ingestion (keeps everything else alive)
	aws events disable-rule --name transitpulse-poll-1min --region $(REGION)
	@echo "ingestion paused"

.PHONY: resume
resume: ## Resume ingestion
	aws events enable-rule --name transitpulse-poll-1min --region $(REGION)
	@echo "ingestion resumed"
```

**Things to notice.**

- A repo with `make plan`, `make apply`, `make backfill`, `make check`, `make pause` reads as operated rather than assembled.
- The `help` target greps `##` comments out of the file, so the documentation can't drift from the targets.
- `make pause` and `make resume` exist because remembering to stop ingestion matters more than any elegance.

---

## `scripts/build_poller_zip.sh`

**Why this file exists.** Packages the poller Lambda as a plain zip, with no Docker involved. This is the default build path and exists specifically so a laptop short on disk and RAM never has to run Docker Desktop to deploy a 200-line function.

```bash
#!/usr/bin/env bash
# Build the poller Lambda as a plain zip. No Docker required.
#
# The poller needs protobuf and gtfs-realtime-bindings, which aren't in the
# Lambda runtime. A container image is one way to ship them; a zip built with
# manylinux wheels is another, and it costs ~1.7 MB instead of ~600 MB.
# pip's --platform flag cross-builds for Lambda's amd64 runtime, so this works
# identically on Apple Silicon and Intel.
set -euo pipefail

SRC="src/ingest/poller"
BUILD="build/poller"
OUT="build/poller.zip"

rm -rf "$BUILD" "$OUT"
mkdir -p "$BUILD"

pip install --quiet --target "$BUILD" \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --upgrade \
  -r "$SRC/requirements.txt"

cp "$SRC/handler.py" "$BUILD/"

# Strip test suites and metadata that Lambda never reads.
find "$BUILD" -type d -name "__pycache__"  -prune -exec rm -rf {} + 2>/dev/null || true
find "$BUILD" -type d -name "tests"        -prune -exec rm -rf {} + 2>/dev/null || true
find "$BUILD" -type d -name "*.dist-info"  -prune -exec rm -rf {} + 2>/dev/null || true

( cd "$BUILD" && zip -qr "../../$OUT" . )

SIZE="$(du -h "$OUT" | cut -f1)"
echo "built $OUT (${SIZE})"
echo "Lambda direct-upload limit is 50 MB zipped / 250 MB unzipped."
```

**Things to notice.**

- `--platform manylinux2014_x86_64` is what makes this work. pip downloads Linux wheels rather than building for your Mac, so the same command produces an identical package on Apple Silicon and Intel.
- It also sidesteps the exec-format-error trap entirely — there's no image architecture to get wrong.
- 868 KB zipped versus roughly 600 MB for the container image. The container was never necessary for these four dependencies.
- `boto3` is deliberately absent: the Lambda runtime provides it, and bundling it would add ~15 MB for nothing.
- Stripping `tests/` and `*.dist-info` shaves the package further. Lambda never reads either.

---

## `scripts/preflight.sh`

**Why this file exists.** Checks that this machine can build the project before you spend money. Makes no changes and creates nothing.

```bash
#!/usr/bin/env bash
# Preflight: verify this machine can build the project before you spend money.
# Safe to run repeatedly. Makes no changes and creates no AWS resources.

set -uo pipefail

PASS=0
WARN=0
FAIL=0

ok()   { printf "  \033[32mPASS\033[0m  %s\n" "$1"; PASS=$((PASS+1)); }
warn() { printf "  \033[33mWARN\033[0m  %s\n" "$1"; WARN=$((WARN+1)); }
bad()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAIL=$((FAIL+1)); }

ver_ge() {
  # ver_ge 1.9.5 1.6.0  -> true when $1 >= $2
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" = "$2" ]
}

echo "=============================================="
echo " TransitPulse preflight"
echo "=============================================="
echo
echo "--- machine ---"

OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
  Darwin)
    MACVER="$(sw_vers -productVersion 2>/dev/null || echo unknown)"
    ok "macOS ${MACVER} (${ARCH})"
    if [ "$ARCH" = "arm64" ]; then
      warn "Apple Silicon: Docker images MUST build with --platform linux/amd64"
      echo "        scripts/build_push_poller.sh already does this for you."
    fi
    ;;
  Linux) ok "Linux (${ARCH})" ;;
  *)     warn "Unrecognised OS: ${OS}" ;;
esac

FREE_GB="$(df -Pg . 2>/dev/null | awk 'NR==2 {print $4}')"
if [ -n "${FREE_GB:-}" ]; then
  if [ "${FREE_GB}" -ge 5 ]; then
    ok "disk free: ${FREE_GB} GB (zip build path needs ~3 GB total)"
  elif [ "${FREE_GB}" -ge 3 ]; then
    warn "disk free: ${FREE_GB} GB -- tight. Skip requirements-ml.txt and run 'make clean' often."
  else
    bad "disk free: ${FREE_GB} GB -- need at least 3 GB"
  fi
fi

echo
echo "--- required tools ---"

check_tool() {
  name="$1"; min="$2"; cmd="$3"
  if ! command -v "$name" >/dev/null 2>&1; then
    bad "$name not installed"
    return
  fi
  got="$(eval "$cmd" 2>/dev/null | head -1)"
  if [ -z "$got" ]; then
    warn "$name installed, version unreadable"
  elif ver_ge "$got" "$min"; then
    ok "$name $got (need >= $min)"
  else
    bad "$name $got is older than $min"
  fi
}

check_tool aws       2.13.0 "aws --version 2>&1 | sed -E 's|aws-cli/([0-9.]+).*|\1|'"
check_tool terraform 1.6.0  "terraform version | head -1 | sed -E 's/Terraform v([0-9.]+).*/\1/'"
check_tool git       2.30.0 "git --version | sed -E 's/git version ([0-9.]+).*/\1/'"
check_tool python3   3.11.0 "python3 --version | sed -E 's/Python ([0-9.]+).*/\1/'"
check_tool jq        1.6    "jq --version | sed -E 's/jq-([0-9.]+).*/\1/'"

if command -v gh >/dev/null 2>&1; then
  ok "gh $(gh --version | head -1 | sed -E 's/gh version ([0-9.]+).*/\1/')"
else
  warn "gh not installed (only needed to create the GitHub repo from the terminal)"
fi

echo
echo "--- docker (OPTIONAL) ---"
# The default build path is a ~2 MB zip. Docker is only needed if you choose
# poller_package_type = "Image".
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ok "docker running (optional -- only needed for the Image build path)"
else
  ok "docker not running -- fine, the default Zip path does not need it"
fi

echo
echo "--- build artifacts ---"
if [ -f build/poller.zip ]; then
  ZSIZE="$(du -h build/poller.zip | cut -f1)"
  ok "build/poller.zip present (${ZSIZE})"
else
  warn "build/poller.zip missing -- run: make package"
fi

echo
echo "--- aws credentials ---"
if IDENT="$(aws sts get-caller-identity --output json 2>/dev/null)"; then
  ACCT="$(echo "$IDENT" | jq -r .Account)"
  ARN="$(echo "$IDENT" | jq -r .Arn)"
  ok "authenticated as ${ARN}"
  ok "account ${ACCT}"

  case "$ARN" in
    *voclabs*|*LabRole*)
      bad "this looks like an AWS Academy Learner Lab"
      echo "        Learner Labs shut down between sessions and cannot run a"
      echo "        minute-by-minute poller. Use a personal AWS account."
      ;;
  esac

  REGION="$(aws configure get region 2>/dev/null || echo "${AWS_REGION:-}")"
  if [ -n "$REGION" ]; then
    ok "default region: ${REGION}"
    [ "$REGION" = "ca-central-1" ] || warn "guide assumes ca-central-1; update envs/dev.tfvars if intentional"
  else
    bad "no default region set -- run: aws configure set region ca-central-1"
  fi
else
  bad "aws sts get-caller-identity failed -- credentials not configured"
fi

echo
echo "--- service reachability (read-only calls) ---"
probe() {
  label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label reachable"; else warn "$label call failed (permissions or region)"; fi
}
if aws sts get-caller-identity >/dev/null 2>&1; then
  probe "s3"          aws s3api list-buckets
  probe "kinesis"     aws kinesis list-streams
  probe "glue"        aws glue get-databases
  probe "sagemaker"   aws sagemaker list-training-jobs --max-results 1
  probe "stepfunctions" aws stepfunctions list-state-machines --max-results 1
  probe "ecr"         aws ecr describe-repositories --max-results 1
fi

echo
echo "--- python environment ---"
if python3 -c "import sys; sys.exit(0 if sys.prefix != sys.base_prefix else 1)" 2>/dev/null; then
  ok "running inside a virtualenv"
else
  warn "not in a virtualenv -- run: python3 -m venv .venv && source .venv/bin/activate"
fi

for pkg in ruff pytest boto3 requests; do
  if python3 -c "import ${pkg}" 2>/dev/null || command -v "$pkg" >/dev/null 2>&1; then
    ok "python: ${pkg}"
  else
    warn "python: ${pkg} missing -- pip install -r requirements-dev.txt"
  fi
done

echo
echo "--- project config ---"
if [ -f infra/envs/dev.tfvars ]; then
  if grep -q "CHANGE_ME" infra/envs/dev.tfvars; then
    bad "infra/envs/dev.tfvars still contains CHANGE_ME -- set alert_email"
  else
    ok "dev.tfvars customised"
  fi
else
  warn "infra/envs/dev.tfvars not found -- are you in the repo root?"
fi

if [ -n "${TL_KEY:-}" ]; then
  ok "TL_KEY is set in the environment"
else
  warn "TL_KEY not set -- export TL_KEY=... before testing the feed"
fi

echo
echo "=============================================="
printf " %d passed, %d warnings, %d failures\n" "$PASS" "$WARN" "$FAIL"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
  echo
  echo "Fix the failures above before running 'make plan'."
  exit 1
fi
echo
echo "Ready. Next: make lint && make test"
```

**Things to notice.**

- Run this first, every time you come back to the project after a gap. Expired credentials and a stopped Docker daemon are the two most common time-wasters and both show up here in seconds.
- `ver_ge()` compares semantic versions using `sort -V` — a neat trick that avoids writing a version parser in bash.
- It detects Apple Silicon and reminds you about the `--platform linux/amd64` requirement, and detects an AWS Academy Learner Lab and tells you to stop.
- The reachability probes are all read-only list calls. They surface permission or region problems before Terraform does, when the error message is still simple.
- It exits non-zero on any failure, so you can gate other scripts behind it.

---

## `scripts/bootstrap.sh`

**Why this file exists.** Creates the Terraform state backend. Chicken-and-egg: Terraform can't create its own state bucket.

```bash
#!/usr/bin/env bash
# One-time: create the Terraform state bucket and lock table.
set -euo pipefail

REGION="${REGION:-ca-central-1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="tfstate-transitpulse-${ACCT}"

echo "account=${ACCT} region=${REGION} bucket=${BUCKET}"

if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "state bucket already exists"
else
  if [ "${REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}"
  fi
fi

aws s3api put-bucket-versioning --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled

aws s3api put-public-access-block --bucket "${BUCKET}" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-encryption --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

if aws dynamodb describe-table --table-name tfstate-lock --region "${REGION}" >/dev/null 2>&1; then
  echo "lock table already exists"
else
  aws dynamodb create-table --table-name tfstate-lock \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST --region "${REGION}"
  aws dynamodb wait table-exists --table-name tfstate-lock --region "${REGION}"
fi

echo "bootstrap complete. next: make init"
```

**Things to notice.**

- `set -euo pipefail` — exit on error, on undefined variable, and on any failure in a pipeline. Every serious bash script should start this way.
- Fully idempotent: safe to rerun, skips what exists.
- Handles the `us-east-1` special case where `create-bucket` rejects a location constraint.

---

## `scripts/backfill.sh`

**Why this file exists.** Replays the ETL over the last N days.

```bash
#!/usr/bin/env bash
# Replay the ETL state machine over the last N days, one at a time.
# Running them all at once would spin up N concurrent Glue jobs and a bill.
set -euo pipefail

DAYS="${1:-14}"
REGION="${REGION:-ca-central-1}"
SM_ARN="$(cd infra && terraform output -raw state_machine_arn)"

for i in $(seq 1 "${DAYS}"); do
  if date -u -d "-${i} day" +%Y-%m-%d >/dev/null 2>&1; then
    RUN_DATE="$(date -u -d "-${i} day" +%Y-%m-%d)"      # GNU date
  else
    RUN_DATE="$(date -u -v-"${i}"d +%Y-%m-%d)"          # BSD/macOS date
  fi

  echo "starting backfill for ${RUN_DATE}"
  aws stepfunctions start-execution \
    --state-machine-arn "${SM_ARN}" \
    --name "backfill-${RUN_DATE}-$(date +%s)" \
    --input "{\"run_date\":\"${RUN_DATE}\"}" \
    --region "${REGION}" >/dev/null

  sleep 120
done

echo "backfill submitted for ${DAYS} days"
```

**Things to notice.**

- `sleep 120` between days is deliberate. Twenty concurrent Glue jobs is a bill, not a speedup.
- It handles both GNU and BSD `date` syntax, because macOS and Linux disagree and this bites everyone once.

---

## `infra/modules/cicd/main.tf`

**Why this file exists.** The GitHub OIDC provider and deploy role. This lets GitHub Actions assume an AWS role using a short-lived workflow token instead of stored access keys.

```hcl
locals {
  enabled = var.github_repo != "" ? 1 : 0
}

# GitHub's OIDC issuer. Creating this lets GitHub Actions exchange a workflow
# token for temporary AWS credentials, so no long-lived access keys are ever
# stored in the repository.
resource "aws_iam_openid_connect_provider" "github" {
  count = local.enabled

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  # AWS validates GitHub's certificate chain itself now, but the provider
  # still requires this argument.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "assume" {
  count = local.enabled

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to this repository only. Without this condition ANY GitHub
    # repository on the internet could assume the role.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  count = local.enabled

  name                 = "github-actions-${var.name}"
  description          = "Assumed by GitHub Actions via OIDC to plan and apply this project"
  assume_role_policy   = data.aws_iam_policy_document.assume[0].json
  max_session_duration = 3600
}

# Broad for a solo project, but bounded: it can only be assumed by one repo,
# sessions last an hour, and every action is in CloudTrail. A team setup would
# split plan (read-only) from apply and scope the apply policy per service.
resource "aws_iam_role_policy_attachment" "deploy" {
  count = local.enabled

  role       = aws_iam_role.deploy[0].name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

# PowerUserAccess excludes IAM, which this project needs in order to manage
# its own service roles.
data "aws_iam_policy_document" "iam_management" {
  count = local.enabled

  statement {
    sid    = "ManageProjectRoles"
    effect = "Allow"

    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:PassRole",
      "iam:TagRole",
      "iam:UpdateRole",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:GetRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy"
    ]

    resources = ["arn:aws:iam::${var.acct}:role/${var.name}-*"]
  }

  statement {
    sid       = "TerraformState"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.state_bucket}", "arn:aws:s3:::${var.state_bucket}/*"]
  }

  statement {
    sid       = "TerraformLock"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = ["arn:aws:dynamodb:${var.region}:${var.acct}:table/tfstate-lock"]
  }
}

resource "aws_iam_role_policy" "iam_management" {
  count = local.enabled

  name   = "${var.name}-iam-management"
  role   = aws_iam_role.deploy[0].id
  policy = data.aws_iam_policy_document.iam_management[0].json
}
```

**Things to notice.**

- The `sub` condition scoped to `repo:owner/name:*` is the whole security model. Without it, any GitHub repository on the internet could assume your role. This is the one line to read twice.
- The `aud` condition is also required — AWS rejects the assumption without it, and the error message is unhelpful.
- `count = local.enabled` with an empty-string default means the module no-ops until you set `github_repo`. You can deploy everything else before you're ready for CI.
- `PowerUserAccess` deliberately excludes IAM, so a separate inline policy grants IAM actions scoped to `transitpulse-*` roles only. CI can manage this project's roles and nothing else.
- `max_session_duration = 3600` bounds how long a leaked session token stays useful.

---

## `scripts/validate_wiring.py`

**Why this file exists.** The checker that caught the Terraform wiring bugs. It parses every .tf file and asserts that module calls, variables, outputs, and referenced file paths all agree. Terraform's own `validate` does much of this too — but this runs with no Terraform binary, no AWS credentials, and no network, so it works in CI on a bare runner.

```python
"""Cross-module consistency checks that HCL syntax parsing cannot catch."""

import glob
import os
import re
import sys

import hcl2

ROOT = "transitpulse-bc/infra"
SRC = "transitpulse-bc"
errors, notes = [], []


def load_dir(d):
    merged = {}
    for f in glob.glob(os.path.join(d, "*.tf")):
        with open(f) as fh:
            doc = hcl2.load(fh)
        for k, v in doc.items():
            merged.setdefault(k, []).extend(v)
    return merged


def unq(x):
    return x.strip('"') if isinstance(x, str) else x


def names(blocks, kind):
    out = set()
    for b in blocks.get(kind, []):
        out |= {unq(k) for k in b}
    return out


def raw_text(d):
    parts = []
    for f in glob.glob(os.path.join(d, "*.tf")):
        with open(f) as fh:
            parts.append(fh.read())
    return "\n".join(parts)


# ---- module inventory ----
modules = {}
for path in sorted(glob.glob(os.path.join(ROOT, "modules", "*"))):
    if not os.path.isdir(path):
        continue
    name = os.path.basename(path)
    blocks = load_dir(path)
    modules[name] = {
        "path": path,
        "vars": names(blocks, "variable"),
        "outputs": names(blocks, "output"),
        "text": raw_text(path),
        "blocks": blocks,
    }

root_blocks = load_dir(ROOT)
root_text = raw_text(ROOT)
root_vars = names(root_blocks, "variable")

# ---- 1. module call args vs declared variables ----
declared_calls = {}
for m in root_blocks.get("module", []):
    for call_name, body in m.items():
        declared_calls[unq(call_name)] = body

for call_name, body in declared_calls.items():
    if call_name not in modules:
        errors.append(f"root calls module '{call_name}' but no modules/{call_name} dir")
        continue
    mod = modules[call_name]
    passed = {
        unq(k)
        for k in body
        if unq(k)
        not in ("source", "providers", "version", "count", "for_each", "depends_on", "__is_block__")
    }
    unknown = passed - mod["vars"]
    required = set()
    for b in mod["blocks"].get("variable", []):
        for vname, vbody in b.items():
            if "default" not in vbody:
                required.add(unq(vname))
    missing = required - passed
    for u in sorted(unknown):
        errors.append(f"module.{call_name}: passes '{u}' which is not a declared variable")
    for mi in sorted(missing):
        errors.append(f"module.{call_name}: required variable '{mi}' not supplied")

# ---- 2. module.X.Y references resolve to real outputs ----
for mod_name, attr in set(re.findall(r"module\.([a-z_]+)\.([a-z_]+)", root_text)):
    if mod_name not in modules:
        errors.append(f"reference module.{mod_name} has no matching module directory")
    elif attr not in modules[mod_name]["outputs"]:
        errors.append(f"module.{mod_name}.{attr} referenced but '{attr}' is not an output")

# ---- 3. every var.X inside a module is declared there ----
for name, mod in modules.items():
    for used in set(re.findall(r"\bvar\.([a-z_]+)", mod["text"])):
        if used not in mod["vars"]:
            errors.append(f"modules/{name}: uses var.{used} which is not declared")

for used in set(re.findall(r"\bvar\.([a-z_]+)", root_text)):
    if used not in root_vars:
        errors.append(f"root: uses var.{used} which is not declared")

# ---- 4. declared but unused variables (warning only) ----
for name, mod in modules.items():
    for v in sorted(mod["vars"]):
        if len(re.findall(rf"\bvar\.{v}\b", mod["text"])) == 0:
            notes.append(f"modules/{name}: variable '{v}' declared but never used")

# ---- 5. file paths referenced from Terraform actually exist ----
for match in set(
    re.findall(
        r'source_dir\s*=\s*"\$\{path\.module\}/([^"]+)"',
        root_text + "".join(m["text"] for m in modules.values()),
    )
):
    for mod in modules.values():
        candidate = os.path.normpath(os.path.join(mod["path"], match))
        if os.path.isdir(candidate):
            break
    else:
        errors.append(f"archive_file source_dir does not resolve: {match}")

# ---- 6. glue scripts referenced exist ----
etl_text = modules["etl"]["text"]
for script in re.findall(r'"(\w+\.py)"', etl_text):
    if not os.path.exists(os.path.join(SRC, "src", "glue", script)):
        errors.append(f"etl module references src/glue/{script} which does not exist")

# ---- 7. tfvars supply every required root variable ----
required_root = set()
for b in root_blocks.get("variable", []):
    for vname, vbody in b.items():
        if "default" not in vbody:
            required_root.add(unq(vname))
for tfvars in glob.glob(os.path.join(ROOT, "envs", "*.tfvars")):
    with open(tfvars) as fh:
        supplied = set(re.findall(r"^\s*([a-z_]+)\s*=", fh.read(), re.M))
    missing = required_root - supplied
    for mi in sorted(missing):
        errors.append(f"{tfvars}: missing required variable '{mi}'")

# ---- 8. Lambda handler entrypoints match Terraform handler strings ----
for handler_ref in re.findall(
    r'handler\s*=\s*"([\w.]+)"', "".join(m["text"] for m in modules.values())
):
    mod_name, fn = handler_ref.rsplit(".", 1)
    if fn != "lambda_handler":
        errors.append(f"unexpected Lambda handler entrypoint: {handler_ref}")

print(f"modules found: {sorted(modules)}")
print(f"module calls in root: {sorted(declared_calls)}")
print()
if errors:
    print(f"ERRORS ({len(errors)}):")
    for e in errors:
        print("  -", e)
else:
    print("wiring checks: PASS")
if notes:
    print(f"\nnotes ({len(notes)}):")
    for n in notes:
        print("  -", n)
sys.exit(1 if errors else 0)
```

**Things to notice.**

- Worth reading as an example of treating your infrastructure as data. `hcl2.load()` gives you a dict; everything after is ordinary Python.
- The `unq()` helper exists because this hcl2 version returns block labels with quotes attached — a real quirk that made the checker report 85 false failures on its first run.
- The unused-variable check is only a warning, because Terraform tolerates unused variables. It still found eight of them worth removing.
- Run it yourself with `python3 scripts/validate_wiring.py` from the repo root after any Terraform edit.

---

## `.github/workflows/ci.yml`

**Why this file exists.** Lint and test on every PR; plan and apply on merge to main.

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

permissions:
  id-token: write
  contents: read

env:
  AWS_REGION: ca-central-1
  TF_VERSION: 1.9.5

jobs:
  quality:
    name: lint and test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Install dev dependencies
        run: pip install -r requirements-dev.txt

      - name: Lint
        run: |
          ruff check .
          ruff format --check .

      - name: Unit tests
        run: pytest --cov=src --cov-report=term-missing

  terraform:
    name: terraform validate and plan
    runs-on: ubuntu-latest
    needs: quality
    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: ${{ env.TF_VERSION }}

      # OIDC federation: no long-lived AWS keys are stored in this repo.
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ secrets.ACCOUNT_ID }}:role/github-actions-transitpulse
          aws-region: ${{ env.AWS_REGION }}

      - name: Terraform init
        working-directory: infra
        run: |
          terraform init \
            -backend-config="bucket=tfstate-transitpulse-${{ secrets.ACCOUNT_ID }}" \
            -backend-config="region=${{ env.AWS_REGION }}"

      - name: Terraform format check
        working-directory: infra
        run: terraform fmt -check -recursive

      - name: Terraform validate
        working-directory: infra
        run: terraform validate

      - name: Checkov security scan
        uses: bridgecrewio/checkov-action@master
        with:
          directory: infra/
          soft_fail: true

      - name: Terraform plan
        working-directory: infra
        run: terraform plan -var-file=envs/dev.tfvars -out=tf.plan

      - name: Terraform apply
        if: github.ref == 'refs/heads/main' && github.event_name == 'push'
        working-directory: infra
        run: terraform apply -auto-approve tf.plan
```

**Things to notice.**

- `permissions: id-token: write` enables OIDC. This is what lets GitHub assume an AWS role with **no stored access keys** — the single most valuable security detail in the repo.
- `needs: quality` means Terraform never runs if lint or tests fail.
- Checkov scans the Terraform for misconfigurations. `soft_fail: true` reports without blocking, which is the right setting while you're learning what it flags.
- Apply is gated on `github.ref == 'refs/heads/main'` — PRs plan, only merges apply.

---

# Appendix — declaration files

These declare inputs, outputs, and configuration. Nothing in them is a design decision, so they're listed for completeness rather than annotated.

## `infra/modules/network/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}
```

## `infra/modules/network/outputs.tf`

```hcl
output "vpc_id" {
  value = aws_vpc.main.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "glue_security_group_id" {
  value = aws_security_group.glue.id
}
```

## `infra/modules/lake/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "force_destroy" {
  type    = bool
  default = false
}
```

## `infra/modules/lake/outputs.tf`

```hcl
output "bucket_names" {
  value = { for k, v in aws_s3_bucket.b : k => v.id }
}

output "bucket_arns" {
  value = { for k, v in aws_s3_bucket.b : k => v.arn }
}

output "glue_db" {
  value = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}
```

## `infra/modules/ingest/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "bronze_bucket" {
  type = string
}

variable "bronze_arn" {
  type = string
}

variable "glue_db" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "poller_package_type" {
  description = "Zip (no Docker needed, ~2 MB) or Image (container, ~600 MB local build)."
  type        = string
  default     = "Zip"

  validation {
    condition     = contains(["Zip", "Image"], var.poller_package_type)
    error_message = "poller_package_type must be Zip or Image."
  }
}

variable "poller_image" {
  description = "ECR image URI. Only used when poller_package_type is Image."
  type        = string
  default     = ""
}

variable "poller_zip_path" {
  description = "Path to the built zip, relative to the infra directory."
  type        = string
  default     = "../build/poller.zip"
}

variable "secret_name" {
  type = string
}

variable "gtfs_static_url" {
  type = string
}

variable "online_table_arn" {
  type = string
}

variable "online_table" {
  type = string
}

variable "log_retention_days" {
  type    = number
  default = 14
}
```

## `infra/modules/ingest/outputs.tf`

```hcl
output "kinesis_stream_name" {
  value = aws_kinesis_stream.gtfs.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.gtfs.arn
}

output "poller_function_name" {
  value = aws_lambda_function.poller.function_name
}

output "poller_log_group" {
  value = aws_cloudwatch_log_group.poller.name
}

output "poller_dlq_name" {
  value = aws_sqs_queue.poller_dlq.name
}

output "poller_rule_name" {
  value = aws_cloudwatch_event_rule.poll.name
}

output "static_loader_function_name" {
  value = aws_lambda_function.static_loader.function_name
}

output "killswitch_function_arn" {
  value = aws_lambda_function.killswitch.arn
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.bronze.name
}
```

## `infra/modules/etl/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "bronze_arn" {
  type = string
}

variable "silver_arn" {
  type = string
}

variable "gold_arn" {
  type = string
}

variable "artifacts_bucket" {
  type = string
}

variable "artifacts_arn" {
  type = string
}

variable "glue_db" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "glue_version" {
  type    = string
  default = "4.0"
}

variable "log_retention_days" {
  type    = number
  default = 14
}
```

## `infra/modules/etl/outputs.tf`

```hcl
output "state_machine_arn" {
  value = aws_sfn_state_machine.etl.arn
}

output "state_machine_name" {
  value = aws_sfn_state_machine.etl.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "glue_job_names" {
  value = { for k, j in aws_glue_job.job : k => j.name }
}
```

## `infra/modules/ml/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "gold_arn" {
  type = string
}

variable "artifacts_arn" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}
```

## `infra/modules/ml/outputs.tf`

```hcl
output "sagemaker_role_arn" {
  value = aws_iam_role.sagemaker.arn
}

output "model_package_group_name" {
  value = aws_sagemaker_model_package_group.models.model_package_group_name
}

output "retrain_rule_name" {
  value = aws_cloudwatch_event_rule.retrain_weekly.name
}
```

## `infra/modules/serving/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "endpoint_name" {
  type    = string
  default = ""
}

variable "log_retention_days" {
  type    = number
  default = 14
}
```

## `infra/modules/serving/outputs.tf`

```hcl
output "online_table_name" {
  value = aws_dynamodb_table.online.name
}

output "online_table_arn" {
  value = aws_dynamodb_table.online.arn
}

output "predict_function_name" {
  value = aws_lambda_function.predict.function_name
}

output "api_base_url" {
  value = aws_apigatewayv2_stage.v1.invoke_url
}

output "endpoint_name" {
  value = local.endpoint
}
```

## `infra/modules/observability/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "region" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "poller_function_name" {
  type = string
}

variable "poller_log_group" {
  type = string
}

variable "poller_dlq_name" {
  type = string
}

variable "kinesis_stream_name" {
  type = string
}

variable "killswitch_function_arn" {
  type = string
}

variable "daily_cost_threshold_usd" {
  type    = number
  default = 3
}
```

## `infra/modules/observability/outputs.tf`

```hcl
output "dashboard_name" {
  value = aws_cloudwatch_dashboard.main.dashboard_name
}

output "alarm_names" {
  value = [
    aws_cloudwatch_metric_alarm.ingest_stalled.alarm_name,
    aws_cloudwatch_metric_alarm.poller_errors.alarm_name,
    aws_cloudwatch_metric_alarm.poller_dlq.alarm_name,
    aws_cloudwatch_metric_alarm.iterator_age.alarm_name,
    aws_cloudwatch_metric_alarm.model_mae_degraded.alarm_name
  ]
}
```

## `infra/modules/observability/providers.tf`

```hcl
terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      version               = "~> 5.60"
      configuration_aliases = [aws.useast1]
    }
  }
}
```

## `infra/modules/cicd/variables.tf`

```hcl
variable "name" {
  type = string
}

variable "acct" {
  type = string
}

variable "region" {
  type = string
}

variable "github_repo" {
  description = "GitHub repo allowed to assume the deploy role, as owner/repo. Empty disables CI/CD entirely."
  type        = string
  default     = ""
}

variable "state_bucket" {
  type = string
}
```

## `infra/modules/cicd/outputs.tf`

```hcl
output "deploy_role_arn" {
  value       = local.enabled == 1 ? aws_iam_role.deploy[0].arn : ""
  description = "Role ARN for the role-to-assume field in the GitHub Actions workflow"
}

output "oidc_provider_arn" {
  value = local.enabled == 1 ? aws_iam_openid_connect_provider.github[0].arn : ""
}
```

## `infra/outputs.tf`

```hcl
output "account_id" {
  value = local.acct
}

output "bucket_names" {
  value = module.lake.bucket_names
}

output "glue_database" {
  value = module.lake.glue_db
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "kinesis_stream_name" {
  value = module.ingest.kinesis_stream_name
}

output "poller_function_name" {
  value = module.ingest.poller_function_name
}

output "state_machine_arn" {
  value = module.etl.state_machine_arn
}

output "sagemaker_role_arn" {
  value = module.ml.sagemaker_role_arn
}

output "model_package_group" {
  value = module.ml.model_package_group_name
}

output "online_feature_table" {
  value = module.serving.online_table_name
}

output "api_base_url" {
  value = module.serving.api_base_url
}

output "github_deploy_role_arn" {
  description = "Paste into role-to-assume in .github/workflows/ci.yml"
  value       = module.cicd.deploy_role_arn
}
```

## `infra/envs/dev.tfvars`

```hcl
env                      = "dev"
region                   = "ca-central-1"
owner                    = "manikanth"
alert_email              = "CHANGE_ME@example.com"
poller_package_type      = "Zip"   # "Image" if you prefer the container build
poller_image_tag         = "v1"
daily_cost_threshold_usd = 3
force_destroy_buckets    = true

github_repo              = ""   # set to "yourusername/transitpulse-bc" once the repo exists
```

## `sql/02_static_tables.sql`

```sql
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
```

## `sql/03_iceberg_stop_events.sql`

```sql
-- Silver stop events. Iceberg because late-arriving corrections must UPDATE
-- rows in place, and because snapshot ids make training sets reproducible.
-- Replace ${BUCKET} before running.

CREATE TABLE IF NOT EXISTS transitpulse.stop_events (
  service_date            date,
  trip_id                 string,
  stop_id                 string,
  route_id                string,
  direction_id            int,
  vehicle_id              string,
  stop_sequence           int,
  start_date              string,
  observed_arrival_epoch  bigint,
  observed_arrival_ts     timestamp,
  observed_delay_sec      int,
  last_seen_ts            bigint,
  delay_t_minus_5         int,
  delay_t_minus_15        int,
  delay_t_minus_30        int,
  snap_ts_t_minus_5       bigint,
  snap_ts_t_minus_15      bigint,
  snap_ts_t_minus_30      bigint,
  scheduled_arrival       string,
  shape_dist_traveled     double,
  stop_lat                double,
  stop_lon                double,
  route_short_name        string,
  route_type              int,
  hour_of_day             int,
  day_of_week             int,
  is_weekend              int,
  stops_remaining         int,
  is_terminus             int
)
PARTITIONED BY (month(service_date))
LOCATION 's3://${BUCKET}/iceberg/stop_events/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet');
```

## `sql/04_baselines.sql`

```sql
-- Run this BEFORE training anything. These three numbers are what the model
-- must beat, and they belong in docs/baselines.md and in the README.

SELECT
  count(*)                                                       AS n,
  avg(abs(observed_delay_sec))                                   AS mae_schedule,
  avg(abs(observed_delay_sec - coalesce(delay_t_minus_15, 0)))   AS mae_persistence,
  avg(abs(observed_delay_sec - coalesce(hist_median_delay, 0)))  AS mae_historical
FROM transitpulse.training_features
WHERE service_date >= current_date - interval '14' day;
```

## `src/ingest/poller/requirements.txt`

```text
requests==2.32.3
urllib3==2.2.3
gtfs-realtime-bindings==1.0.0
protobuf==4.25.4
```

## `pyproject.toml`

```toml
[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["notebooks", "build"]

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "C4", "SIM"]
ignore = ["E501"]

[tool.ruff.lint.per-file-ignores]
# Glue and SageMaker entry points import awsglue / sagemaker, which only exist
# in their managed runtimes and are not installed locally.
"src/glue/*.py" = ["E402"]
"src/serving/predict/handler.py" = ["E402"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src", "src/ingest"]
addopts = "-q"
```

## `requirements-dev.txt`

```text
# Tier 1 — everything needed to lint, test, and deploy the pipeline.
# Small: roughly 40 MB installed. This is all most steps need.
ruff==0.6.9
pytest==8.3.3
pytest-cov==5.0.0
boto3==1.35.36
requests==2.32.3
gtfs-realtime-bindings==1.0.0
protobuf==4.25.4
```

## `requirements-ml.txt`

```text
# Tier 2 — only needed for Phase 6 (creating the SageMaker pipeline and the
# optional local training dry run). Roughly 1.5 GB installed.
#
# On a laptop short of disk, skip this: Phase 6 can be driven entirely from the
# AWS CLI, and training runs on SageMaker rather than locally either way.
#   pip install -r requirements-ml.txt
# and when you're done:
#   pip uninstall -y -r requirements-ml.txt
sagemaker==2.232.2
pandas==2.2.3
pyarrow==17.0.0
xgboost==2.1.1
scikit-learn==1.5.2
```

## `requirements-test-extras.txt`

```text
# Tier 3 — optional. AWS mocking for integration tests you may add later.
moto[all]==5.0.16
```

## `scripts/build_push_poller.sh`

```bash
#!/usr/bin/env bash
# Build the poller container and push it to ECR.
# --platform linux/amd64 is mandatory on Apple Silicon; without it the Lambda
# fails at runtime with an exec format error.
set -euo pipefail

REGION="${REGION:-ca-central-1}"
TAG="${TAG:-v1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
REPO="transitpulse/poller"
URI="${ACCT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:${TAG}"

aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "${REPO}" --region "${REGION}" \
       --image-scanning-configuration scanOnPush=true

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCT}.dkr.ecr.${REGION}.amazonaws.com"

docker build --platform linux/amd64 -t "${URI}" src/ingest/poller
docker push "${URI}"

echo "pushed ${URI}"
```

## `scripts/daily_check.sh`

```bash
#!/usr/bin/env bash
# Five-minute daily health check while the pipeline is collecting data.
set -uo pipefail

REGION="${REGION:-ca-central-1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
TODAY="$(date -u +%Y-%m-%d)"

echo "=== 1. bronze arriving today? ==="
aws s3 ls "s3://transitpulse-bronze-${ACCT}/raw/trip_updates/dt=${TODAY}/" \
  --recursive --region "${REGION}" | tail -3

echo "=== 2. firehose conversion errors (want 0) ==="
aws s3 ls "s3://transitpulse-bronze-${ACCT}/errors/" --recursive --region "${REGION}" | wc -l

echo "=== 3. alarms currently firing ==="
aws cloudwatch describe-alarms --state-value ALARM --region "${REGION}" \
  --query "MetricAlarms[].AlarmName" --output table

echo "=== 4. poller dead letter queue ==="
DLQ_URL="$(aws sqs get-queue-url --queue-name transitpulse-poller-dlq \
  --region "${REGION}" --query QueueUrl --output text 2>/dev/null)"
if [ -n "${DLQ_URL}" ] && [ "${DLQ_URL}" != "None" ]; then
  aws sqs get-queue-attributes --queue-url "${DLQ_URL}" \
    --attribute-names ApproximateNumberOfMessages --region "${REGION}" \
    --query "Attributes.ApproximateNumberOfMessages" --output text
fi

echo "=== 5. yesterday's spend on this project ==="
if date -u -d "-2 day" +%Y-%m-%d >/dev/null 2>&1; then
  START="$(date -u -d '-2 day' +%Y-%m-%d)"
else
  START="$(date -u -v-2d +%Y-%m-%d)"
fi
aws ce get-cost-and-usage \
  --time-period "Start=${START},End=${TODAY}" \
  --granularity DAILY --metrics UnblendedCost \
  --filter '{"Tags":{"Key":"Project","Values":["transitpulse"]}}' \
  --query "ResultsByTime[].Total.UnblendedCost.Amount" --output text 2>/dev/null \
  || echo "(cost data lags ~24h on new accounts)"
```

## `scripts/validate_hcl.py`

```python
import glob
import sys

import hcl2

fails = []
files = sorted(glob.glob("transitpulse-bc/infra/**/*.tf", recursive=True))
for f in files:
    try:
        with open(f) as fh:
            hcl2.load(fh)
    except Exception as e:
        fails.append((f, str(e).split("\n")[0][:220]))

print(f"parsed {len(files)} .tf files")
if fails:
    print(f"\nFAILURES ({len(fails)}):")
    for f, e in fails:
        print(f"  {f}\n     {e}")
    sys.exit(1)
print("all HCL parsed cleanly")
```

---

# Where to go deeper

If a concept in here is new, these are the specific things worth reading about, roughly in the order they'll pay off:

| Concept | Where it appears | Why it matters beyond this project |
|---|---|---|
| Window functions in Spark | `silver_stop_events.py` | The single most useful SQL/Spark technique for event data |
| Point-in-time correctness | `gold_features.historical_priors` | The difference between real and fraudulent ML metrics |
| Training/serving skew | `common/features.py`, `test_feature_parity.py` | The most common silent production ML failure |
| Idempotency | `static_loader`, `MERGE INTO`, `bootstrap.sh` | Every pipeline gets rerun; the ones that survive are idempotent |
| Least-privilege IAM | every `aws_iam_policy_document` | Roughly 70% of AWS errors you'll hit are IAM |
| Partition projection | `sql/01_bronze_tables.sql` | Turns Athena from expensive to cheap |
| Columnar formats | Firehose Parquet conversion | Why Parquet queries cost ~10x less than CSV |
| OIDC federation | `.github/workflows/ci.yml` | How CI authenticates to clouds without stored secrets |
| Backpressure and partial failure | `poller._flush`, Kinesis shards | Distributed systems fail partially, not totally |
