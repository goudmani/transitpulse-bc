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
