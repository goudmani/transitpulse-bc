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
