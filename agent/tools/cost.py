"""Cost Explorer tools.

Three things about this account that are easy to get wrong and expensive to miss,
and all three fail the same way: they return a plausible $0.00 rather than an
error, so a runaway bill reads as healthy.

1. Cost Explorer answers only on the global endpoint in us-east-1. Called in
   ca-central-1 it returns an empty result set rather than an error.

2. **An unfiltered query nets credits against usage.** This account carries
   Credit, Tax and Usage record types, and free-tier credits currently cover
   essentially all usage -- so summing every record type reports ~$0.00 on a day
   that actually consumed $10.33 of resources. Every query here therefore filters
   to RECORD_TYPE = Usage. Gross usage is the number that answers "is this
   pipeline wasteful", which is the question being asked; what the invoice says
   after credits is a different question, and credits run out.

3. A filter on the `Project` cost allocation tag matches nothing until that tag
   is activated in Billing -> Cost allocation tags, and activation does not
   backfill. So the default is account-wide; see config.COST_FILTER_BY_TAG.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from langchain_core.tools import tool

from agent import config
from agent.tools._aws import aws_error, client, money, ok, problem, record, table


def _ce():
    return client("ce", region=config.CE_REGION)


# Usage only. Without this, Credit lines net the total to zero on a
# credit-covered account and the agent reports a $10 day as $0.00.
_USAGE_ONLY = {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}}


def _filter() -> dict:
    if not config.COST_FILTER_BY_TAG:
        return _USAGE_ONLY
    return {
        "And": [
            _USAGE_ONLY,
            {"Tags": {"Key": "Project", "Values": [config.RESOURCE_PREFIX]}},
        ]
    }


def _daily_by_service(days: int) -> dict[str, dict[str, float]]:
    """{date: {service: cost}} for the last `days` complete days."""
    # End is exclusive, and today is always partial, so the window stops at
    # today. Cost data also lags roughly 24h, which is why the most recent day
    # in the result can legitimately be missing or low.
    end = datetime.now(UTC).date()
    start = end - timedelta(days=days)
    out: dict[str, dict[str, float]] = {}
    resp = _ce().get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        Filter=_filter(),
    )
    for period in resp["ResultsByTime"]:
        day = period["TimePeriod"]["Start"]
        out[day] = {}
        for group in period["Groups"]:
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            # Sub-cent line items are noise and there are dozens of them.
            if amount >= 0.005:
                out[day][group["Keys"][0]] = amount
    return out


@tool
def get_daily_cost(days: int = 7) -> str:
    """Daily spend for the last N days, broken down by AWS service, with the
    day-over-day change for each service.

    Start every cost investigation here. The per-service delta column is what
    identifies a runaway: a total that moved without any single service moving
    is normal variance, a total that moved because one service tripled is not.
    """
    days = max(2, min(days, 14))
    try:
        data = _daily_by_service(days)
    except Exception as exc:
        return aws_error("get_cost_and_usage", exc)

    if not data:
        return problem(
            "Cost Explorer returned no data. If COST_FILTER_BY_TAG is true this "
            "almost certainly means the Project tag is not activated in Billing -> "
            "Cost allocation tags (activation does not backfill). Report as info, "
            "not as a cost finding."
        )

    dates = sorted(data)
    totals = {d: sum(data[d].values()) for d in dates}
    rows = [[d, money(totals[d])] for d in dates]
    lines = [f"daily totals (last {len(dates)} days):", table(["date", "total"], rows)]

    if len(dates) >= 2:
        latest, prior = dates[-1], dates[-2]
        services = sorted(set(data[latest]) | set(data[prior]))
        delta_rows = []
        for s in services:
            now, before = data[latest].get(s, 0.0), data[prior].get(s, 0.0)
            delta_rows.append(
                [s[:38], money(before), money(now), f"{now - before:+.2f}"]
            )
        delta_rows.sort(key=lambda r: -abs(float(r[3])))
        lines += [
            f"\nper-service, {prior} -> {latest}:",
            table([" service", prior, latest, "delta"], delta_rows, max_rows=12),
        ]
        movers = [r for r in delta_rows if abs(float(r[3])) >= config.SERVICE_DELTA_THRESHOLD]
        if movers:
            lines.append(
                problem(
                    "services moved more than "
                    f"{money(config.SERVICE_DELTA_THRESHOLD)} day over day: "
                    + ", ".join(r[0].strip() for r in movers)
                )
            )

    lines.append(
        "\nNote: cost data lags ~24h, so the most recent day may be incomplete "
        "and reading low. Do not report a low final day as a saving."
    )
    return "\n".join(lines)


@tool
def check_cost_thresholds() -> str:
    """Compare the latest complete day and the month-to-date run rate against the
    configured thresholds, and project the month-end total.

    This is the tool that decides whether cost is a finding. It does the
    arithmetic deterministically -- do not recompute the projection yourself,
    quote what this returns.
    """
    try:
        data = _daily_by_service(8)
    except Exception as exc:
        return aws_error("get_cost_and_usage", exc)
    if not data:
        return problem("Cost Explorer returned no data; cannot evaluate thresholds.")

    dates = sorted(data)
    totals = {d: sum(data[d].values()) for d in dates}

    # The most recent day is usually still settling, so the threshold check runs
    # against the last *complete* day. With fewer than two days of data there is
    # nothing complete to check.
    if len(dates) < 2:
        return problem("only one day of cost data available; need two to judge a trend.")
    complete_day = dates[-2]
    complete_total = totals[complete_day]

    recent = [totals[d] for d in dates[-4:-1]] or [complete_total]
    run_rate = sum(recent) / len(recent)

    today = datetime.now(UTC).date()
    days_in_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    projected_month = run_rate * days_in_month.day

    lines = [
        f"last complete day ({complete_day}): {money(complete_total)} "
        f"vs threshold {money(config.DAILY_COST_THRESHOLD)}",
        # Shown alongside because the threshold check necessarily lags a day, and
        # without this a breach on a day that has since been fixed reads as a
        # live emergency. If the newer day is far lower, say so rather than
        # letting the reader discover it three sections later.
        f"most recent day ({dates[-1]}, may still be settling): {money(totals[dates[-1]])}",
        f"3-day run rate: {money(run_rate)}/day",
        f"projected month at that rate: {money(projected_month)} "
        f"vs threshold {money(config.MONTHLY_COST_THRESHOLD)}",
    ]

    breached = False
    if complete_total > config.DAILY_COST_THRESHOLD:
        breached = True
        over = complete_total - config.DAILY_COST_THRESHOLD
        top = sorted(data[complete_day].items(), key=lambda kv: -kv[1])[:3]
        lines.append(
            problem(
                f"DAILY THRESHOLD BREACHED by {money(over)}. Top services that day: "
                + ", ".join(f"{k} {money(v)}" for k, v in top)
            )
        )
    if projected_month > config.MONTHLY_COST_THRESHOLD:
        breached = True
        lines.append(
            problem(
                f"MONTHLY PROJECTION BREACHED by "
                f"{money(projected_month - config.MONTHLY_COST_THRESHOLD)}."
            )
        )
    if not breached:
        lines.append(ok("both daily and projected-monthly spend are within threshold"))

    # Free-tier credits can cover the whole bill, which makes the invoice read
    # $0.00 while the pipeline still consumes real resources. Reporting gross
    # usage without saying so invites "but AWS charged me nothing" -- and reporting
    # only the net would hide a wasteful pipeline until the credits ran out.
    credited = _credits_applied(complete_day)
    if credited > 0.005:
        lines.append(
            f"\nNote: {money(credited)} of credits were applied on {complete_day}, "
            f"so the invoice for that day is near zero. The figures above are gross "
            f"usage, which is what says whether the pipeline is wasteful."
        )

    return record("cost_thresholds", "\n".join(lines))


def _credits_applied(day: str) -> float:
    """Credits applied on one day, as a positive number. 0.0 if unavailable."""
    try:
        resp = _ce().get_cost_and_usage(
            TimePeriod={"Start": day, "End": (date.fromisoformat(day) + timedelta(days=1)).isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            Filter={"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit"]}},
        )
        amount = float(resp["ResultsByTime"][0]["Total"]["UnblendedCost"]["Amount"])
        return abs(amount)
    except Exception:
        # Purely informational, so a failure here must not fail the threshold check.
        return 0.0


@tool
def get_cost_drivers(day: str = "") -> str:
    """Full per-service breakdown for one day, cheapest line items dropped.
    Pass a YYYY-MM-DD date, or leave empty for the most recent complete day.

    Call this only after check_cost_thresholds reports a breach -- it exists to
    attribute an overrun to a service, not to browse.
    """
    try:
        data = _daily_by_service(8)
    except Exception as exc:
        return aws_error("get_cost_and_usage", exc)
    if not data:
        return problem("Cost Explorer returned no data.")

    dates = sorted(data)
    target = day if day in data else (dates[-2] if len(dates) >= 2 else dates[-1])
    services = sorted(data[target].items(), key=lambda kv: -kv[1])
    total = sum(v for _, v in services)
    rows = [[s[:44], money(v), f"{100 * v / total:.0f}%"] for s, v in services]
    return f"{target} total {money(total)}\n" + table(["service", "cost", "share"], rows)


COST_TOOLS = [get_daily_cost, check_cost_thresholds, get_cost_drivers]
