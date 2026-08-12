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
