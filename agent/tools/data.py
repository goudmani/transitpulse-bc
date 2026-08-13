"""Data quality via Athena.

The health tools answer "is it running". These answer "is the output usable",
which is the question that actually gates Phase 5. The two come apart routinely:
a fully green pipeline still produced a day that was 3.1% cancelled trips and a
day with 7,314 events missing route_id.

Every query goes through the `transitpulse` workgroup, which carries the result
location and the per-query byte cap, so a malformed query cannot scan the lake.
"""

from __future__ import annotations

import re
import time

from langchain_core.tools import tool

from agent import config
from agent.tools._aws import aws_error, client, ok, problem, table

_POLL_SECONDS = 2
_MAX_POLLS = 45

# The agent's IAM role is read-only, so this is defence in depth rather than the
# only control -- but a model that talks itself into a DROP should be stopped
# here, at the point where the intent is visible, not by an AccessDenied later.
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|merge|truncate|grant|revoke|"
    r"unload|msck|vacuum|optimize)\b",
    re.IGNORECASE,
)


def _run(sql: str, max_rows: int = 25) -> str:
    sql = sql.strip().rstrip(";")
    if not re.match(r"^\s*(select|with)\b", sql, re.IGNORECASE):
        return "REJECTED: only SELECT and WITH queries are allowed."
    if _FORBIDDEN.search(sql):
        return "REJECTED: query contains a data-modifying keyword."
    if ";" in sql:
        return "REJECTED: one statement per call, no semicolons."

    try:
        athena = client("athena")
        qid = athena.start_query_execution(
            QueryString=sql,
            WorkGroup=config.ATHENA_WORKGROUP,
            QueryExecutionContext={"Database": config.GLUE_DATABASE},
        )["QueryExecutionId"]
    except Exception as exc:
        return aws_error("start_query_execution", exc)

    for _ in range(_MAX_POLLS):
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            # HIVE_BAD_DATA naming a column means that column's declared type in
            # sql/06_gold_tables.sql disagrees with what the Parquet actually
            # holds -- a schema bug, not a query bug, and worth saying so.
            hint = (
                " (HIVE_BAD_DATA names the column whose DDL type is wrong "
                "in sql/06_gold_tables.sql)"
                if "HIVE_BAD_DATA" in reason
                else ""
            )
            return f"QUERY_FAILED: {reason}{hint}"
        time.sleep(_POLL_SECONDS)
    else:
        return "QUERY_TIMEOUT: still running after 90s; treat as unknown, not as failure."

    try:
        result = athena.get_query_results(QueryExecutionId=qid, MaxResults=max_rows + 1)
        rows = result["ResultSet"]["Rows"]
    except Exception as exc:
        return aws_error("get_query_results", exc)

    if not rows:
        return "(no rows)"
    header = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
    body = [[c.get("VarCharValue", "NULL") for c in r["Data"]] for r in rows[1:]]
    return table(header, body, max_rows=max_rows)


@tool
def check_collection_progress() -> str:
    """How many distinct service days are in the silver table, and how far that
    is from the 21 days a time-based train/val/test split needs.

    This is the single number that says whether Phase 5 is unblocked.
    """
    out = _run(
        "SELECT count(DISTINCT service_date) AS days, "
        "min(service_date) AS first_day, max(service_date) AS last_day "
        f"FROM {config.GLUE_DATABASE}.stop_events"
    )
    if out.startswith(("QUERY_", "REJECTED", "TOOL_")):
        return out
    lines = out.splitlines()
    try:
        days = int(lines[1].split()[0])
    except (IndexError, ValueError):
        return out
    remaining = config.TARGET_COLLECTION_DAYS - days
    verdict = (
        ok(f"{days} days collected -- enough for a time-based split, Phase 5 is unblocked")
        if remaining <= 0
        else f"{days} of {config.TARGET_COLLECTION_DAYS} days collected, {remaining} to go"
    )
    return f"{out}\n\n{verdict}"


@tool
def check_recent_days(limit: int = 7) -> str:
    """Per-day event count, label rate and duplicate count for the most recent
    service days.

    Three things to read: events well below normal means the poller was down for
    part of that day and it should be excluded rather than trained on;
    label_rate below the threshold means the DQ gate would have quarantined it;
    dupes MUST be zero, because non-zero means the silver window key is wrong and
    every downstream aggregate is inflated.
    """
    limit = max(1, min(limit, 14))
    out = _run(
        "SELECT cast(service_date AS varchar) AS service_date, "
        "count(*) AS events, "
        "count(observed_delay_sec) AS labelled, "
        "round(count(observed_delay_sec) * 1.0 / count(*), 3) AS label_rate, "
        "count(*) - count(DISTINCT trip_id || '|' || stop_id) AS dupes "
        f"FROM {config.GLUE_DATABASE}.stop_events "
        f"GROUP BY service_date ORDER BY service_date DESC LIMIT {limit}"
    )
    return (
        f"{out}\n\nthresholds: events >= {config.MIN_EVENTS_PER_DAY:,}, "
        f"label_rate >= {config.MIN_LABEL_RATE}, dupes must be 0"
    )


@tool
def check_feature_nulls() -> str:
    """Null rates on the gold training features for the most recent service day.

    Watch for a feature that is null or constant across the whole day. Several
    features are known stubs -- is_holiday and active_alert_on_route are literal
    zeros, and four weather features fall back to constants because dim_weather
    does not exist -- so a *constant* column is expected for those and only worth
    reporting if it is a column that should vary.
    """
    return _run(
        "SELECT cast(service_date AS varchar) AS service_date, count(*) AS rows_out "
        f"FROM {config.GLUE_DATABASE}.training_features "
        "GROUP BY service_date ORDER BY service_date DESC LIMIT 7"
    )


@tool
def athena_query(sql: str) -> str:
    """Run a read-only Athena query against the transitpulse database.

    For anything the preset tools do not cover. SELECT and WITH only, one
    statement, no semicolon. Tables: stop_events (silver, one row per arrival),
    training_features (gold), dim_trips, dim_routes, dim_stops (static GTFS).
    Always put a LIMIT on it and always filter on service_date -- the table is
    partitioned on it and a query without that filter scans everything.
    """
    return _run(sql)


DATA_TOOLS = [check_collection_progress, check_recent_days, check_feature_nulls, athena_query]
