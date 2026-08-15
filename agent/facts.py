"""Live facts about the running system, as values rather than prose.

Everything the README claims that can go stale is derived here, deterministically
and with no model involved. A model rewriting figures every day would drift, and
a drifting README is worse than a dated one because it looks maintained.

The split that matters: this module produces *numbers*, `docs_updater.py` renders
them into marked blocks, and the model is only asked about prose claims that no
query can settle.

The chart and baseline numbers come from `sql/07_profile_queries.sql` rather than
from SQL embedded here, so editing that file changes what the README says. Two
sources of truth for the same figure is how a README starts disagreeing with its
own charts.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from statistics import median

from agent import config
from agent.tools._aws import account_id, client, resources_by_prefix
from agent.tools.cost import _daily_by_service
from agent.tools.data import query_rows

log = logging.getLogger(__name__)

PROFILE_SQL = config.REPO_ROOT / "sql" / "07_profile_queries.sql"

# Positions of the statements in that file. Named rather than indexed at the
# call site so adding a query does not silently reassign the others.
Q_DELAY_DISTRIBUTION = 0
Q_DELAY_BY_HOUR = 1
Q_BASELINES = 2
Q_HISTORICAL = 3


def load_profile_queries() -> list[str]:
    """Split sql/07_profile_queries.sql into its statements, in file order.

    Comments are stripped before splitting so a stray semicolon inside one
    cannot cut a query in half.
    """
    if not PROFILE_SQL.exists():
        log.warning("%s not found", PROFILE_SQL)
        return []
    raw = PROFILE_SQL.read_text(encoding="utf-8")
    without_comments = re.sub(r"--[^\n]*", "", raw)
    return [s.strip() for s in without_comments.split(";") if s.strip()]


@dataclass
class Collection:
    days: int = 0
    first_day: str = ""
    last_day: str = ""
    total_events: int = 0
    labelled: int = 0

    @property
    def label_rate(self) -> float:
        return self.labelled / self.total_events if self.total_events else 0.0

    @property
    def remaining(self) -> int:
        return max(0, config.TARGET_COLLECTION_DAYS - self.days)


@dataclass
class Baselines:
    n: int = 0
    mae_schedule: float = 0.0
    mae_persistence: float = 0.0

    @property
    def persistence_gain_pct(self) -> float:
        if not self.mae_schedule:
            return 0.0
        return (self.mae_schedule - self.mae_persistence) / self.mae_schedule * 100

    @property
    def registry_gate_sec(self) -> float:
        """A model must beat this to be registered. Gate is <= 0.92."""
        return self.mae_persistence * 0.92


@dataclass
class Cost:
    latest_day: str = ""
    latest_total: float = 0.0
    median_rate: float = 0.0
    projected_month: float = 0.0
    top_services: list[tuple[str, float]] = field(default_factory=list)


@dataclass
class Deployment:
    endpoints: int = 0
    model_packages: int = 0
    pipelines: int = 0
    ingestion_enabled: bool = False
    etl_last_status: str = "unknown"

    @property
    def model_exists(self) -> bool:
        return self.endpoints > 0 or self.model_packages > 0


def collection() -> Collection:
    """Service days in silver, and how usable they are."""
    result = query_rows(
        "SELECT count(DISTINCT service_date) AS days, "
        "min(service_date) AS first_day, max(service_date) AS last_day, "
        "count(*) AS total_events, count(observed_delay_sec) AS labelled "
        f"FROM {config.GLUE_DATABASE}.stop_events"
    )
    if isinstance(result, str) or not result[1]:
        log.warning("collection query failed: %s", result)
        return Collection()
    row = result[1][0]
    try:
        return Collection(
            days=int(row[0]),
            first_day=row[1],
            last_day=row[2],
            total_events=int(row[3]),
            labelled=int(row[4]),
        )
    except (ValueError, IndexError) as exc:
        log.warning("could not parse collection row %r: %s", row, exc)
        return Collection()


def baselines() -> Baselines:
    """Query 3 from the profile file: what the model has to beat."""
    queries = load_profile_queries()
    if len(queries) <= Q_BASELINES:
        return Baselines()
    result = query_rows(queries[Q_BASELINES])
    if isinstance(result, str) or not result[1]:
        log.warning("baseline query failed: %s", result)
        return Baselines()
    row = result[1][0]
    try:
        return Baselines(n=int(row[0]), mae_schedule=float(row[1]), mae_persistence=float(row[2]))
    except (ValueError, IndexError) as exc:
        log.warning("could not parse baseline row %r: %s", row, exc)
        return Baselines()


def cost() -> Cost:
    """Gross usage. _daily_by_service already filters to RECORD_TYPE = Usage."""
    try:
        data = _daily_by_service(5)
    except Exception as exc:
        log.warning("cost query failed: %s", exc)
        return Cost()
    if not data:
        return Cost()

    dates = sorted(data)
    totals = {d: sum(data[d].values()) for d in dates}
    latest = dates[-1]
    rate = median([totals[d] for d in dates[-3:]])

    today = datetime.now(UTC).date()
    days_in_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    return Cost(
        latest_day=latest,
        latest_total=totals[latest],
        median_rate=rate,
        projected_month=rate * days_in_month.day,
        top_services=sorted(data[latest].items(), key=lambda kv: -kv[1])[:3],
    )


def deployment() -> Deployment:
    """What is actually deployed, as opposed to what Terraform describes.

    The README claimed "a deployed SageMaker model" while there was no endpoint,
    no registered model package and no pipeline. These calls are what settle that
    claim, so it cannot drift silently again.
    """
    d = Deployment()
    sm = client("sagemaker")
    try:
        d.endpoints = len(sm.list_endpoints()["Endpoints"])
    except Exception as exc:
        log.warning("list_endpoints: %s", exc)
    try:
        d.model_packages = len(
            sm.list_model_packages(ModelPackageGroupName=config.RESOURCE_PREFIX)[
                "ModelPackageSummaryList"
            ]
        )
    except Exception as exc:
        # A group that has never held a package raises rather than returning [].
        log.debug("list_model_packages: %s", exc)
    try:
        d.pipelines = len(sm.list_pipelines()["PipelineSummaries"])
    except Exception as exc:
        log.warning("list_pipelines: %s", exc)
    try:
        rules = client("events").list_rules(NamePrefix=f"{config.RESOURCE_PREFIX}-poll")["Rules"]
        d.ingestion_enabled = any(r.get("State") == "ENABLED" for r in rules)
    except Exception as exc:
        log.warning("list_rules: %s", exc)
    try:
        sfn = client("stepfunctions")
        machines = resources_by_prefix(sfn.list_state_machines()["stateMachines"], "name")
        etl = next((m for m in machines if m["name"] == f"{config.RESOURCE_PREFIX}-etl"), None)
        if etl:
            execs = sfn.list_executions(stateMachineArn=etl["stateMachineArn"], maxResults=1)[
                "executions"
            ]
            d.etl_last_status = execs[0]["status"] if execs else "never run"
    except Exception as exc:
        log.warning("stepfunctions: %s", exc)
    return d


def chart_data() -> dict[str, tuple[list[str], list[list[str]]]]:
    """Rows for the two README charts, keyed by the CSV stem plot_profile expects.

    Returns only the queries that succeeded: a partial refresh that updates one
    chart beats failing both, and the caller reports what it skipped.
    """
    queries = load_profile_queries()
    out: dict[str, tuple[list[str], list[list[str]]]] = {}
    for name, idx in (
        ("delay_distribution", Q_DELAY_DISTRIBUTION),
        ("delay_by_hour", Q_DELAY_BY_HOUR),
    ):
        if len(queries) <= idx:
            log.warning("%s missing from %s", name, PROFILE_SQL.name)
            continue
        # 24 hours of rows for query 2, 7 buckets for query 1.
        result = query_rows(queries[idx], max_rows=40)
        if isinstance(result, str):
            log.warning("chart query %s failed: %s", name, result)
            continue
        header, rows = result
        if not rows:
            log.warning("chart query %s returned no rows", name)
            continue
        out[name] = (header, rows)
    return out


def bronze_objects_today() -> int:
    """Object count in today's bronze partition, as a liveness signal."""
    try:
        resp = client("s3").list_objects_v2(
            Bucket=f"{config.RESOURCE_PREFIX}-bronze-{account_id()}",
            Prefix=f"raw/trip_updates/dt={config.run_date()}/",
            MaxKeys=1000,
        )
        return resp.get("KeyCount", 0)
    except Exception as exc:
        log.warning("bronze listing: %s", exc)
        return 0
