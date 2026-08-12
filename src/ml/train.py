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
