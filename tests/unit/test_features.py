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
