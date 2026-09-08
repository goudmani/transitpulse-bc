"""Load hourly weather for the collection window into bronze as dim_weather.

Four of the model's 26 features -- temp_c, precipitation_mm, wind_kph,
visibility_m -- come from this table. Until it exists, join_weather() in
gold_features.py raises, the try/except fills constants, and those four columns
carry no information at all.

TIMEZONE, which is the whole trap here: gold joins weather on
    today.hour_of_day == weather.weather_hour
and hour_of_day is now LOCAL (from_utc_timestamp, America/Vancouver). So the API
is asked for local timestamps too. Request UTC instead and the join still
succeeds, silently, seven hours out of alignment -- every row would carry the
weather from the middle of the previous night.

    python scripts/load_weather.py --start 2026-08-11 --end 2026-09-06
"""

from __future__ import annotations

import argparse
import subprocess
import sys

import pandas as pd
import requests

VANCOUVER = {"latitude": 49.2827, "longitude": -123.1207}
TIMEZONE = "America/Vancouver"
ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = ["temperature_2m", "precipitation", "wind_speed_10m", "visibility"]

# gold_features.py falls back to these when the join misses. Matching them here
# means a gap in the API's coverage produces the same value as no table at all,
# rather than a null that behaves differently downstream.
FALLBACK = {"visibility": 20000.0}


def fetch(start: str, end: str) -> pd.DataFrame:
    params = {
        **VANCOUVER,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(HOURLY),
        "timezone": TIMEZONE,
    }
    response = requests.get(ARCHIVE, params=params, timeout=90)
    response.raise_for_status()
    payload = response.json()

    if "hourly" not in payload:
        sys.exit(f"no hourly block in response: {str(payload)[:400]}")

    frame = pd.DataFrame(payload["hourly"])

    # ERA5 does not carry every variable the forecast API does. Fill rather than
    # fail -- three real weather features beat none.
    for column in HOURLY:
        if column not in frame.columns:
            fill = FALLBACK.get(column)
            if fill is None:
                sys.exit(f"{column} missing from the response and has no fallback")
            print(f"  {column}: absent from archive, filling {fill}")
            frame[column] = fill

        # Pin float64 instead of letting pandas infer. visibility comes back as
        # whole metres (24140, 21000), which pandas types as int64 and Parquet
        # stores as INT32 -- and Spark's vectorised reader refuses to widen that
        # to the double the Athena DDL declares:
        #   Parquet column cannot be converted ... Column: [visibility],
        #   Expected: double, Found: INT32
        # The failure lands at gold-rebuild time, an hour downstream of here.
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")

    return frame[["time", *HOURLY]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--bucket", default="")
    args = ap.parse_args()

    bucket = args.bucket or (
        "transitpulse-bronze-"
        + subprocess.run(
            ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )

    print(f"fetching {args.start} .. {args.end} for {TIMEZONE}")
    frame = fetch(args.start, args.end)

    # Coverage per day, so a partial archive is visible now rather than as
    # unexplained nulls after a 90-minute gold rebuild.
    frame["day"] = frame["time"].str.slice(0, 10)
    coverage = frame.groupby("day")[HOURLY[0]].apply(lambda s: s.notna().sum())
    thin = coverage[coverage < 24]
    print(f"\n{len(frame):,} hourly rows across {coverage.size} days")
    if not thin.empty:
        print("days with fewer than 24 hours of data:")
        for day, hours in thin.items():
            print(f"  {day}: {hours}/24")
    else:
        print("all days complete (24/24)")

    frame = frame.drop(columns=["day"])
    out = "/tmp/weather.parquet"
    frame.to_parquet(out, index=False)

    key = f"s3://{bucket}/static/weather/weather.parquet"
    subprocess.run(["aws", "s3", "cp", out, key], check=True)
    print(f"\nuploaded {key}")

    print(
        "\nNow register it in Athena:\n\n"
        "CREATE EXTERNAL TABLE IF NOT EXISTS transitpulse.dim_weather (\n"
        "  time            string,\n"
        "  temperature_2m  double,\n"
        "  precipitation   double,\n"
        "  wind_speed_10m  double,\n"
        "  visibility      double\n"
        ")\n"
        "STORED AS PARQUET\n"
        f"LOCATION 's3://{bucket}/static/weather/';\n"
    )


if __name__ == "__main__":
    main()
