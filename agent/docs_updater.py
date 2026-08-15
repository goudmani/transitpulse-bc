"""Keeps the README's figures and charts current.

    python -m agent.docs_updater

Runs later in the day than the ops agent, because it wants the nightly ETL's
output to have landed in silver first.

Two jobs, and the division between them is the whole design:

**Numbers are rendered, not written.** Everything between `agent:*:begin` and
`agent:*:end` markers is regenerated from live queries with no model involved.
A model asked to "update the figures" produces plausible numbers, drifts a
little each day, and eventually states something false in a document that looks
maintained. Rendering is boring and correct.

**Prose is checked, not rewritten.** The claims a query cannot settle -- "a
deployed SageMaker model that beats the published schedule" -- are handed to a
model that compares them against live facts and *reports* contradictions. It
does not edit them. A human fixes the sentence, because a sentence is an
argument and rewriting it needs to be a decision.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from datetime import UTC, datetime

from agent import config, facts, tracing

log = logging.getLogger("docs")

README = config.REPO_ROOT / "README.md"
CSV_DIR = config.REPO_ROOT / "data" / "processed"
IMG_DIR = config.REPO_ROOT / "img"


# --- marked block rendering ------------------------------------------------


def replace_block(text: str, name: str, body: str) -> tuple[str, bool]:
    """Swap the content between agent:<name>:begin/end markers.

    Returns (text, changed). A missing marker pair is a warning rather than an
    error: the README is edited by hand too, and losing a day's figures beats
    corrupting the file by guessing where the block should have gone.
    """
    pattern = re.compile(
        rf"(<!-- agent:{re.escape(name)}:begin -->\n)(.*?)(\n<!-- agent:{re.escape(name)}:end -->)",
        re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        log.warning("no agent:%s block found in README", name)
        return text, False
    if match.group(2).strip() == body.strip():
        return text, False
    return pattern.sub(lambda m: m.group(1) + body.strip() + m.group(3), text, count=1), True


def render_status(col: facts.Collection, dep: facts.Deployment, cost: facts.Cost) -> str:
    """The Status block: what is running, and how far collection has got."""
    ingest = "running continuously" if dep.ingestion_enabled else "**PAUSED**"
    etl = {
        "SUCCEEDED": "running continuously",
        "FAILED": "**last run FAILED**",
        "RUNNING": "running now",
        "never run": "**never run**",
    }.get(dep.etl_last_status, dep.etl_last_status)

    if dep.endpoints:
        serving = f"{dep.endpoints} endpoint(s) live"
    else:
        serving = "infrastructure live, no model behind it"
    registry = (
        f"{dep.model_packages} model package(s) registered"
        if dep.model_packages
        else "provisioned in Terraform, never run"
    )

    half = "and the model half is not" if not dep.model_exists else "and a model is registered"
    lines = [
        f"**As of {config.run_date()}, the data half is in production {half}.**",
        "",
        "| Stage | State |",
        "|---|---|",
        f"| Ingestion, bronze, silver, gold | {ingest} |",
        f"| Nightly ETL | {etl} (last execution {dep.etl_last_status}) |",
        "| Nightly ops agent | running |",
        f"| Training, evaluation, model registry | {registry} |",
        f"| Inference endpoint, prediction API | {serving} |",
        "",
    ]

    if not dep.model_exists:
        lines.append(
            "Nothing is trained yet, so no model has been registered and no endpoint "
            "exists. Phase 5 splits train/validation/test by time, which needs "
            f"{config.TARGET_COLLECTION_DAYS} distinct service days; "
            f"**{col.days} are collected**"
            + (f", {col.remaining} to go" if col.remaining else " — Phase 5 is unblocked")
            + "."
        )
    else:
        lines.append(
            f"{col.days} service days collected, {col.total_events:,} stop arrivals, "
            f"label completeness {col.label_rate:.3f}."
        )

    if cost.latest_day:
        lines += [
            "",
            f"Gross usage on {cost.latest_day} was **${cost.latest_total:,.2f}**, "
            f"a ${cost.median_rate:,.2f}/day median over the last three days "
            f"(≈${cost.projected_month:,.0f}/month at that rate).",
        ]
    return "\n".join(lines)


def render_baselines(base: facts.Baselines, col: facts.Collection) -> str:
    """The baseline MAE table. Every number here comes from query 3."""
    if not base.n:
        return "Baseline figures unavailable: the baseline query returned no rows."

    span = (
        f"{col.first_day} to {col.last_day}"
        if col.first_day and col.last_day
        else f"{col.days} service days"
    )
    caveat = (
        "**Preliminary**: too few days to cover a weekend, rain, or an incident."
        if col.days < config.TARGET_COLLECTION_DAYS
        else f"Computed over the full {col.days}-day training window."
    )
    historical = (
        "pending, needs ≥5 days of history"
        if col.days < 5
        else "see `sql/07_profile_queries.sql` query 4"
    )

    return "\n".join(
        [
            f"Over {base.n:,} labelled stop arrivals, {span}. {caveat} Figures come "
            "from `sql/07_profile_queries.sql`.",
            "",
            "| Predictor | MAE (seconds) |",
            "|---|---|",
            f"| Published schedule (predict zero delay) | **{base.mae_schedule:.1f}** |",
            f"| Persistence (bus stays as late as it currently is) | "
            f"**{base.mae_persistence:.1f}** |",
            f"| Historical median for route/stop/hour | {historical} |",
            "| **XGBoost model** | pending, Phase 6 |",
            "",
            f"Persistence beats the printed timetable by "
            f"{base.persistence_gain_pct:.1f}%. The registry gate is "
            f"`mae_ratio_vs_persistence <= 0.92`, so a model must reach "
            f"**≤ {base.registry_gate_sec:.1f} seconds** to be registered at all.",
        ]
    )


def render_dataprofile(col: facts.Collection) -> str:
    return (
        f"{col.total_events:,} stop arrivals over {col.days} days of collection "
        f"({col.first_day} to {col.last_day}), label completeness "
        f"{col.label_rate:.3f}."
    )


# --- charts ----------------------------------------------------------------


def refresh_charts() -> list[str]:
    """Re-run the chart queries, write the CSVs, redraw the PNGs.

    scripts/plot_profile.py reads from data/processed/, which is gitignored --
    derived data is regenerable and does not belong in history. That is exactly
    why the charts went stale: nothing automatic could rebuild the inputs.
    """
    data = facts.chart_data()
    if not data:
        log.warning("no chart data returned; charts left untouched")
        return []

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    for name, (header, rows) in data.items():
        path = CSV_DIR / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)
        log.info("wrote %s (%d rows)", path.relative_to(config.REPO_ROOT), len(rows))

    # Imported here, not at module scope: matplotlib and pandas are heavy, and
    # the marked-block update is useful even on a machine without them.
    sys.path.insert(0, str(config.REPO_ROOT / "scripts"))
    try:
        import plot_profile
    except ImportError as exc:
        log.warning("cannot redraw charts (%s); CSVs are still refreshed", exc)
        return []

    written = []
    IMG_DIR.mkdir(exist_ok=True)
    for fn, out in (
        (plot_profile.delay_distribution, "delay_distribution.png"),
        (plot_profile.hourly_profile, "hourly_profile.png"),
    ):
        try:
            fn()
            written.append(f"img/{out}")
        except Exception as exc:
            # One chart failing must not lose the other, or the README update.
            log.warning("chart %s failed: %s", out, exc)
    return written


# --- prose drift -----------------------------------------------------------

_DRIFT_PROMPT = """You check a project README for factual claims that have become
false. You do not rewrite it and you do not improve the writing.

Verified live facts about the system right now:

{facts}

Below is the README's prose. Blocks between `agent:*:begin` and `agent:*:end`
have already been stripped -- they are regenerated automatically and are correct
by construction.

Report ONLY sentences that a fact above proves wrong. Quote each sentence
VERBATIM; if you cannot copy it exactly from the text, it is not a finding.

Not drift. Do not report these:
- Rounded or approximate figures close to the facts ("~16M rows" against 15.8M).
- Design rationale, trade-offs, opinions, or explanations of why something was
  chosen. A number cannot contradict an argument.
- Anything framed as future or pending ("will", "planned", "Phase 6").
- Baseline MAE figures. Baselines are computed from collected data and are NOT
  model results; their presence does not mean a model exists.
- Wording you would have phrased differently.

An empty list is the correct and expected answer on most days.

README:
---
{readme}
---"""


def check_prose_drift(text: str, fact_summary: str) -> str:
    """Ask a model which prose claims contradict reality. Report only, never edit.

    Structured output, and deliberately on the higher-reasoning client: judging
    whether a sentence is contradicted is exactly the kind of multi-step call
    that low effort does badly. The first unstructured version hedged,
    contradicted itself mid-sentence, and invented findings to fill space.
    Requiring a verbatim quote makes fabrication much harder -- the sentence has
    to exist in the text.
    """
    from agent.llm import code_llm
    from agent.schemas import DriftReport

    # Marked blocks are stripped: they are correct by construction, and leaving
    # them in invites the model to "find" drift in the agent's own output.
    stripped = re.sub(
        r"<!-- agent:\w+:begin -->.*?<!-- agent:\w+:end -->", "", text, flags=re.DOTALL
    )
    prompt = _DRIFT_PROMPT.format(facts=fact_summary, readme=stripped[:14000])
    try:
        report = code_llm().with_structured_output(DriftReport).invoke(
            prompt, config=tracing.child_config("docs:prose-drift", "docs")
        )
    except Exception as exc:
        log.warning("drift check failed: %s", exc)
        return f"(drift check failed: {type(exc).__name__})"

    if not report or not report.claims:
        return "NO DRIFT"

    # Verify each quote actually appears. A claim about a sentence that is not in
    # the file is a hallucination, and dropping it here costs nothing.
    out = []
    for c in report.claims:
        needle = " ".join(c.quote.split())[:60]
        if needle and needle.lower() not in " ".join(stripped.split()).lower():
            log.warning("dropping drift claim, quote not found in README: %r", needle)
            continue
        out.append(f"- **{c.confidence}** — \"{c.quote.strip()}\"\n  {c.contradicts.strip()}")
    return "\n\n".join(out) if out else "NO DRIFT"


def _fact_summary(col, base, cost, dep) -> str:
    return "\n".join(
        [
            f"- Service days collected: {col.days} of {config.TARGET_COLLECTION_DAYS}",
            f"- Stop arrivals in silver: {col.total_events:,}",
            f"- Label completeness: {col.label_rate:.3f}",
            f"- MAE, published schedule: {base.mae_schedule:.1f}s",
            f"- MAE, persistence: {base.mae_persistence:.1f}s",
            f"- Persistence beats schedule by: {base.persistence_gain_pct:.1f}%",
            f"- SageMaker endpoints deployed: {dep.endpoints}",
            f"- Registered model packages: {dep.model_packages}",
            f"- SageMaker pipelines: {dep.pipelines}",
            f"- Ingestion EventBridge rule enabled: {dep.ingestion_enabled}",
            f"- Last ETL execution: {dep.etl_last_status}",
            f"- Gross usage on {cost.latest_day}: ${cost.latest_total:,.2f}",
            f"- 3-day median usage: ${cost.median_rate:,.2f}/day",
        ]
    )


# --- entry point -----------------------------------------------------------


@tracing.traced_run("transitpulse-docs-update")
def run(skip_charts: bool = False, skip_drift: bool = False) -> dict:
    col = facts.collection()
    base = facts.baselines()
    cost = facts.cost()
    dep = facts.deployment()

    if not col.days:
        # Rendering zeros over good figures would be worse than doing nothing.
        log.error("no collection data; refusing to overwrite the README with zeros")
        return {"changed": False, "reason": "no data", "charts": [], "drift": ""}

    text = README.read_text(encoding="utf-8")
    original = text
    for name, body in (
        ("status", render_status(col, dep, cost)),
        ("baselines", render_baselines(base, col)),
        ("dataprofile", render_dataprofile(col)),
    ):
        text, changed = replace_block(text, name, body)
        log.info("block %-12s %s", name, "updated" if changed else "unchanged")

    charts = [] if skip_charts else refresh_charts()
    drift = "" if skip_drift else check_prose_drift(original, _fact_summary(col, base, cost, dep))

    if text != original:
        README.write_text(text, encoding="utf-8")

    result = {
        "changed": text != original,
        "charts": charts,
        "drift": drift,
        "days": col.days,
        "trace_url": tracing.current_trace_url(),
    }

    if drift and drift != "NO DRIFT" and not drift.startswith("("):
        # Written beside the daily reports so drift is reviewable in one place.
        config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = config.REPORTS_DIR / f"{config.run_date()}-docs-drift.md"
        path.write_text(
            f"# README drift check — {config.run_date()}\n\n"
            f"Claims that appear to contradict live facts. **Not auto-corrected**: "
            f"a sentence is an argument, and rewriting one should be a decision.\n\n"
            f"{drift}\n\n---\n\nFacts at "
            f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}:\n\n```\n"
            f"{_fact_summary(col, base, cost, dep)}\n```\n",
            encoding="utf-8",
        )
        result["drift_report"] = str(path.relative_to(config.REPO_ROOT))

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh README figures and charts")
    parser.add_argument("--skip-charts", action="store_true", help="markers only, no matplotlib")
    parser.add_argument("--skip-drift", action="store_true", help="no model call at all")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
    )
    if not args.skip_drift:
        tracing.configure()

    try:
        result = run(skip_charts=args.skip_charts, skip_drift=args.skip_drift)
    finally:
        tracing.flush()

    log.info(
        "README %s, %d chart(s) redrawn, drift: %s",
        "updated" if result["changed"] else "unchanged",
        len(result["charts"]),
        result["drift"][:80] or "(skipped)",
    )

    if out := os.getenv("GITHUB_OUTPUT"):
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if result['changed'] else 'false'}\n")
            fh.write(f"charts={len(result['charts'])}\n")
            fh.write(f"has_drift={'true' if result.get('drift_report') else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
