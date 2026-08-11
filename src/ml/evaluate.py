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
