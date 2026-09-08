"""Score held-out test rows with the trained model and emit the demo dataset.

The static demo page has no backend, so every number it shows has to be computed
here, once, and baked into the page. Nothing is simulated: the predictions come
from the registered model artifact, the labels come from the test split the model
never saw, and the baselines are recomputed on the identical rows so the
comparison on the page is the same comparison the registry gate made.

Usage:
    python scripts/build_demo_data.py \
        --model-dir /tmp/tpdemo \
        --test-glob '/tmp/feat/test/*.parquet' \
        --out docs/demo-data.json
"""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

TARGET = "observed_delay_sec"

# Same fill values as src/ml/train.py, so the page scores rows exactly the way
# the training and evaluation jobs did. Diverging here would make the demo a
# different model than the one that passed the gate.
DEFAULTS = {
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
    "shape_dist_traveled": 0.0,
    "temp_c": 10.0,
    "precipitation_mm": 0.0,
    "wind_kph": 10.0,
    "visibility_m": 20000.0,
    "active_alert_on_route": 0.0,
}

SAMPLE_ROWS = 1500
TOP_ROUTES = 12


def load(model_dir: Path, test_glob: str) -> tuple[xgb.Booster, list[str], pd.DataFrame]:
    booster = xgb.Booster()
    booster.load_model(str(model_dir / "xgboost-model.json"))
    features = json.loads((model_dir / "feature_names.json").read_text())

    files = sorted(glob.glob(test_glob))
    if not files:
        raise FileNotFoundError(f"no parquet under {test_glob}")
    frame = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    frame = frame.dropna(subset=[TARGET])
    return booster, features, frame


def score(booster: xgb.Booster, features: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    matrix = frame[features].copy()
    for column, default in DEFAULTS.items():
        if column in matrix:
            matrix[column] = pd.to_numeric(matrix[column], errors="coerce").fillna(default)
    for column in features:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce").fillna(0.0)

    frame = frame.copy()
    frame["predicted"] = booster.predict(xgb.DMatrix(matrix[features]))
    frame["err_model"] = (frame[TARGET] - frame["predicted"]).abs()
    frame["err_persistence"] = (frame[TARGET] - frame["delay_t_minus_15"].fillna(0)).abs()
    frame["err_historical"] = (frame[TARGET] - frame["hist_median_delay"].fillna(0)).abs()
    frame["err_schedule"] = frame[TARGET].abs()
    return frame


def headline(frame: pd.DataFrame) -> dict:
    return {
        "n_scored": int(len(frame)),
        "mae_model": round(float(frame["err_model"].mean()), 2),
        "mae_persistence": round(float(frame["err_persistence"].mean()), 2),
        "mae_historical": round(float(frame["err_historical"].mean()), 2),
        "mae_schedule": round(float(frame["err_schedule"].mean()), 2),
        "rmse_model": round(float(np.sqrt(((frame[TARGET] - frame["predicted"]) ** 2).mean())), 2),
        "p50_abs_error": round(float(frame["err_model"].quantile(0.5)), 2),
        "p90_abs_error": round(float(frame["err_model"].quantile(0.9)), 2),
        "within_60s": round(float((frame["err_model"] <= 60).mean()), 4),
        "within_120s": round(float((frame["err_model"] <= 120).mean()), 4),
        "first_day": str(frame["service_date"].min()),
        "last_day": str(frame["service_date"].max()),
    }


def by_hour(frame: pd.DataFrame) -> list[dict]:
    grouped = frame.groupby("hour_of_day")
    return [
        {
            "hour": int(hour),
            "n": int(len(group)),
            "model": round(float(group["err_model"].mean()), 1),
            "persistence": round(float(group["err_persistence"].mean()), 1),
            "historical": round(float(group["err_historical"].mean()), 1),
            "schedule": round(float(group["err_schedule"].mean()), 1),
            "mean_delay": round(float(group[TARGET].mean()), 1),
        }
        for hour, group in grouped
    ]


def by_route(frame: pd.DataFrame) -> list[dict]:
    counts = frame["route_short_name"].value_counts().head(TOP_ROUTES)
    rows = []
    for route in counts.index:
        group = frame[frame["route_short_name"] == route]
        rows.append(
            {
                "route": str(route),
                "n": int(len(group)),
                "model": round(float(group["err_model"].mean()), 1),
                "persistence": round(float(group["err_persistence"].mean()), 1),
                "historical": round(float(group["err_historical"].mean()), 1),
                "schedule": round(float(group["err_schedule"].mean()), 1),
                "mean_delay": round(float(group[TARGET].mean()), 1),
            }
        )
    return sorted(rows, key=lambda r: r["model"])


def error_histogram(frame: pd.DataFrame) -> list[dict]:
    """Signed error, so the page can show bias as well as spread."""
    signed = (frame["predicted"] - frame[TARGET]).clip(-600, 600)
    counts, edges = np.histogram(signed, bins=48, range=(-600, 600))
    return [
        {"lo": int(edges[i]), "hi": int(edges[i + 1]), "n": int(counts[i])}
        for i in range(len(counts))
    ]


def importance(booster: xgb.Booster, features: list[str]) -> list[dict]:
    gains = booster.get_score(importance_type="gain")
    total = sum(gains.values()) or 1.0
    ranked = sorted(gains.items(), key=lambda kv: kv[1], reverse=True)
    return [
        {"feature": name, "gain": round(value / total, 4)}
        for name, value in ranked
        if name in features
    ][:15]


def sample(frame: pd.DataFrame) -> list[dict]:
    """Stratify by hour so the page is explorable across the whole day, not just
    the rush hours that dominate the row count."""
    per_hour = max(1, SAMPLE_ROWS // frame["hour_of_day"].nunique())
    picked = (
        frame.groupby("hour_of_day", group_keys=False)[frame.columns.tolist()]
        .apply(lambda g: g.sample(min(len(g), per_hour), random_state=42))
        # Shuffled after stratifying. groupby emits the hours in order, so an
        # unshuffled sample opens the demo page on 40 consecutive midnight
        # arrivals -- which reads as a broken filter, not a fair sample.
        .sample(frac=1.0, random_state=7)
        .reset_index(drop=True)
    )
    def num(value, digits: int = 1, default=None):
        """NaN must not reach the JSON. json.dumps emits a bare NaN token, which
        is not valid JSON and makes JSON.parse throw -- the page would fail to
        load entirely rather than degrade. `value or default` does not help:
        NaN is truthy, so it passes straight through.
        """
        if value is None or pd.isna(value):
            return default
        return round(float(value), digits)

    return [
        {
            "route": str(row.route_short_name),
            "stop": str(row.stop_id),
            "date": str(row.service_date),
            "hour": int(row.hour_of_day),
            "weekend": int(row.is_weekend),
            "peak": int(row.is_peak),
            "seq": int(row.stop_sequence),
            "remaining": int(row.stops_remaining),
            "actual": int(row.observed_delay_sec),
            "pred": num(row.predicted),
            "persistence": num(row.delay_t_minus_15),
            "historical": num(row.hist_median_delay),
            "rain": num(row.precipitation_mm, 2, 0.0),
            "temp": num(row.temp_c),
        }
        for row in picked.itertuples()
    ]


def write_profile_csvs(frame: pd.DataFrame, out_dir: Path) -> None:
    """Refresh the two CSVs the README charts are built from.

    These were previously exported from Athena over `stop_events`, whose
    hour_of_day was UTC -- so plot_profile.py compensated with a hard-coded -7.
    Regenerating them here from the gold split means the hours are already
    Vancouver local and the charts, the model metrics and the demo page all
    describe the same rows.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    buckets = [
        ("1. early >5m", frame[TARGET] < -300),
        ("2. early 1-5m", frame[TARGET].between(-300, -60, inclusive="left")),
        ("3. on time", frame[TARGET].between(-60, 60, inclusive="both")),
        ("4. late 1-3m", frame[TARGET].between(60, 180, inclusive="right")),
        ("5. late 3-5m", frame[TARGET].between(180, 300, inclusive="right")),
        ("6. late 5-10m", frame[TARGET].between(300, 600, inclusive="right")),
        ("7. late >10m", frame[TARGET] > 600),
    ]
    pd.DataFrame(
        [{"bucket": name, "n": int(mask.sum())} for name, mask in buckets]
    ).to_csv(out_dir / "delay_distribution.csv", index=False)

    hourly = (
        frame.groupby("hour_of_day")
        .agg(
            events=(TARGET, "size"),
            avg_delay=(TARGET, "mean"),
            p90_delay=(TARGET, lambda s: s.quantile(0.9)),
        )
        .round({"avg_delay": 1, "p90_delay": 0})
        .reset_index()
    )
    hourly.to_csv(out_dir / "delay_by_hour.csv", index=False)
    print(f"refreshed {out_dir}/delay_distribution.csv and delay_by_hour.csv")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", default="/tmp/tpdemo")
    parser.add_argument("--test-glob", default="/tmp/feat/test/*.parquet")
    parser.add_argument("--out", default="docs/demo-data.json")
    parser.add_argument("--csv-dir", default="data/processed")
    args = parser.parse_args()

    booster, features, frame = load(Path(args.model_dir), args.test_glob)
    frame = score(booster, features, frame)

    payload = {
        "headline": headline(frame),
        "by_hour": by_hour(frame),
        "by_route": by_route(frame),
        "error_histogram": error_histogram(frame),
        "importance": importance(booster, features),
        "samples": sample(frame),
    }

    write_profile_csvs(frame, Path(args.csv_dir))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # allow_nan=False turns a stray NaN into a loud error here rather than a
    # silently unparseable file that only fails in the browser.
    out.write_text(json.dumps(payload, separators=(",", ":"), allow_nan=False))
    print(json.dumps(payload["headline"], indent=2))
    print(f"\nwrote {out} ({out.stat().st_size / 1024:.0f} KB), "
          f"{len(payload['samples'])} sample rows")


if __name__ == "__main__":
    main()
