"""Shared boto3 plumbing and formatting helpers.

Formatting is not cosmetic here. A raw `get_metric_data` response is a few
thousand tokens of timestamps and floats; the same information as a one-line
summary is about thirty. On a 12K tokens-per-minute budget that difference is
what decides whether the run finishes, so every tool in this package returns
compact text and never a dumped API response.
"""

from __future__ import annotations

import functools
from datetime import datetime
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from agent import config

# Retries absorb the throttling that comes from five agents hitting CloudWatch
# in the same minute. Standard mode already backs off exponentially.
_BOTO_CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})


@functools.cache
def client(service: str, region: str | None = None):
    """Cached boto3 client. Cached because a fresh client re-reads credentials
    and re-resolves the endpoint on every construction, which adds up when the
    tools are called dozens of times in a run."""
    return boto3.client(service, region_name=region or config.REGION, config=_BOTO_CONFIG)


@functools.lru_cache(maxsize=1)
def account_id() -> str:
    return client("sts").get_caller_identity()["Account"]


# Output from the handful of tools that carry the numbers a human actually tracks
# daily. The supervisor renders these into the report verbatim, so the headline
# figures appear whether or not the model chose to quote them in its findings.
#
# This exists because it did not work the other way round: told to put its key
# numbers in `facts`, the cost agent reported "costs are within thresholds" with
# no figure at all. Asking a model to remember something is weaker than taking it
# from the tool that already produced it.
_RECORDED: dict[str, str] = {}


def record(key: str, output: str) -> str:
    """Store a tool's output under `key` and return it unchanged."""
    _RECORDED[key] = output
    return output


def recorded(key: str) -> str:
    return _RECORDED.get(key, "")


def ok(msg: str) -> str:
    return f"OK: {msg}"


def problem(msg: str) -> str:
    return f"PROBLEM: {msg}"


def aws_error(action: str, exc: Exception) -> str:
    """Turn an exception into something an agent can act on.

    An AccessDenied is a different problem from a service being down, and the
    agent needs to be able to tell them apart -- otherwise a missing IAM
    permission gets reported as a broken pipeline.
    """
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        if code in ("AccessDenied", "AccessDeniedException", "UnauthorizedOperation"):
            return (
                f"TOOL_UNAVAILABLE: {action} was denied by IAM ({code}). This is a "
                f"permissions gap in the agent's role, NOT a pipeline failure. "
                f"Report it as an info finding and move on."
            )
        return f"TOOL_ERROR: {action} failed with {code}: {exc}"
    return f"TOOL_ERROR: {action} failed: {type(exc).__name__}: {exc}"


def metric_series(
    namespace: str,
    metric_name: str,
    dimensions: dict[str, str],
    stat: str,
    period: int,
    start: datetime,
    end: datetime,
) -> list[float]:
    """One metric as a plain list of values, oldest first.

    Missing datapoints are simply absent rather than zero-filled. CloudWatch does
    not emit a zero for "nothing happened" -- it emits nothing -- and treating
    the gap as a zero would make a paused poller look like a poller returning no
    records, which are different failures.
    """
    resp = client("cloudwatch").get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m0",
                "MetricStat": {
                    "Metric": {
                        "Namespace": namespace,
                        "MetricName": metric_name,
                        "Dimensions": [{"Name": k, "Value": v} for k, v in dimensions.items()],
                    },
                    "Period": period,
                    "Stat": stat,
                },
                "ReturnData": True,
            }
        ],
        StartTime=start,
        EndTime=end,
        ScanBy="TimestampAscending",
    )
    results = resp.get("MetricDataResults", [])
    return [float(v) for v in results[0]["Values"]] if results else []


def metric_total(namespace: str, metric_name: str, dimensions: dict[str, str], **kw) -> float:
    return sum(metric_series(namespace, metric_name, dimensions, **kw))


def metric_max(namespace: str, metric_name: str, dimensions: dict[str, str], **kw) -> float:
    values = metric_series(namespace, metric_name, dimensions, **kw)
    return max(values) if values else 0.0


def resources_by_prefix(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Filter an AWS listing down to this project's resources.

    The agent's role is scoped to the account, not to the project, so an account
    that hosts anything else would otherwise have unrelated Lambdas reported as
    TransitPulse failures.
    """
    return [i for i in items if str(i.get(key, "")).startswith(config.RESOURCE_PREFIX)]


def table(headers: list[str], rows: list[list[Any]], max_rows: int = 25) -> str:
    """A fixed-width table. Models read these more reliably than nested JSON and
    they cost roughly half the tokens."""
    if not rows:
        return "(no rows)"
    total = len(rows)
    cells = [[str(c) for c in r] for r in rows[:max_rows]]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    out += ["  ".join(c.ljust(widths[i]) for i, c in enumerate(r)) for r in cells]
    if total > max_rows:
        out.append(f"... ({max_rows} of {total} rows shown)")
    return "\n".join(out)


def money(x: float) -> str:
    return f"${x:,.2f}"
