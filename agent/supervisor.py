"""The main agent: runs the specialists, compiles the daily report, files it.

Run it with `python -m agent.supervisor`.

The division of labour here is deliberate. The subagents judge; the supervisor
mostly does not. Findings, statuses and numbers are carried through verbatim
from the structured responses, and the only thing the supervisor model is asked
to produce is the two-sentence summary at the top. That way a model having an
off day can make the summary bland, but it cannot silently drop a critical
finding or invent a number that never came from a tool.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime

from agent import config, tracing
from agent.llm import shared_llm, verify_model
from agent.schemas import CodeReport, Finding, SubagentReport
from agent.subagents import build_code_agent, build_subagents, run_subagent

log = logging.getLogger("agent")

_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}
_STATUS_ICON = {"healthy": "green", "degraded": "amber", "broken": "red"}

_TASKS = {
    "infrastructure": (
        "Run today's infrastructure health check on the TransitPulse pipeline "
        "and report what you find."
    ),
    "cost": (
        "Run today's cost check. Compare spend against the thresholds and "
        "attribute anything over them to a service."
    ),
    "data_quality": (
        "Run today's data quality check. Report collection progress toward the "
        "target and flag any unusable service days."
    ),
}


def _severity_counts(reports: dict[str, SubagentReport]) -> dict[str, int]:
    counts = {"critical": 0, "warning": 0, "info": 0}
    for r in reports.values():
        for f in r.findings:
            counts[f.severity] = counts.get(f.severity, 0) + 1
    return counts


def _overall_status(reports: dict[str, SubagentReport]) -> str:
    statuses = {r.status for r in reports.values()}
    if "broken" in statuses:
        return "broken"
    if "degraded" in statuses:
        return "degraded"
    return "healthy"


def _code_task(reports: dict[str, SubagentReport]) -> str:
    """Hand the code agent only what it needs: the problems, not the whole run.

    Passing the full transcripts would be both expensive and counterproductive --
    given three healthy reports in full, the model reliably finds something to
    'improve'. Given a short list of actual problems, it stays on task.
    """
    problems: list[str] = []
    for name, r in reports.items():
        for f in r.findings:
            if f.severity in ("critical", "warning"):
                problems.append(
                    f"- [{f.severity}] ({name}) {f.title}: {f.detail} " f"| evidence: {f.evidence}"
                )
    if not problems:
        return (
            "The infrastructure, cost and data quality agents all reported "
            "healthy with no findings today. Confirm there is nothing to "
            "investigate and return status healthy with no findings and no "
            "patches. Do not go looking for code to change."
        )
    return (
        "Today's other agents reported the following problems. Trace any of them "
        "that point at a defect in this repository, and propose a fix only where "
        "you can read the offending line.\n\n" + "\n".join(problems)
    )


def _summary(reports: dict[str, SubagentReport], code: CodeReport) -> str:
    """Two sentences from the model, over the structured findings only."""
    counts = _severity_counts({**reports, "code": code})
    digest = json.dumps(
        {
            name: {"status": r.status, "headline": r.headline}
            for name, r in {**reports, "code": code}.items()
        },
        indent=None,
    )
    prompt = (
        "You are writing the opening of a daily operations report for a "
        "streaming data pipeline. Here is what each specialist agent reported:\n\n"
        f"{digest}\n\n"
        f"Finding counts: {counts}.\n\n"
        "Write exactly two sentences for an engineer reading this over coffee. "
        "First sentence: is the pipeline fine, and if not what is broken. "
        "Second sentence: the one thing worth doing today, or say explicitly "
        "that no action is needed. No preamble, no bullet points, no markdown."
    )
    try:
        return (
            shared_llm()
            .invoke(prompt, config=tracing.child_config("supervisor:summary", "supervisor"))
            .content.strip()
        )
    except Exception as exc:
        log.warning("summary generation failed: %s", exc)
        # The report is still worth filing without its prose summary.
        return (
            f"Overall status: {_overall_status({**reports, 'code': code})}. "
            f"Findings: {counts['critical']} critical, {counts['warning']} warning. "
            f"(Summary generation failed: {type(exc).__name__}.)"
        )


def _key_numbers() -> str:
    """The figures a human tracks daily, taken from the tools rather than the model.

    Empty when a tool did not run -- an absent section is honest, a fabricated
    one is not.
    """
    from agent.tools._aws import recorded

    blocks = [
        recorded("cost_thresholds"),
        recorded("collection_progress"),
    ]
    return "\n\n".join(b for b in blocks if b)


def _render_findings(findings: list[Finding]) -> str:
    if not findings:
        return "_No findings._\n"
    out = []
    for f in sorted(findings, key=lambda f: _SEVERITY_ORDER.get(f.severity, 9)):
        out.append(f"**[{f.severity.upper()}] {f.title}**\n")
        out.append(f"{f.detail}\n")
        if f.evidence:
            out.append(f"- Evidence: {f.evidence}")
        if f.suggested_action:
            out.append(f"- Action: {f.suggested_action}")
        out.append("")
    return "\n".join(out)


def build_report(
    reports: dict[str, SubagentReport],
    code: CodeReport,
    summary: str,
    trace_url: str = "",
) -> str:
    date = config.run_date()
    counts = _severity_counts({**reports, "code": code})
    overall = _overall_status({**reports, "code": code})

    lines = [
        f"# TransitPulse daily report — {date}",
        "",
        f"**Status: {overall} ({_STATUS_ICON[overall]})** · "
        f"{counts['critical']} critical · {counts['warning']} warning · "
        f"{counts['info']} info",
        "",
        summary,
        "",
        "| Agent | Status | Headline |",
        "| --- | --- | --- |",
    ]
    for name, r in {**reports, "code": code}.items():
        headline = r.headline.replace("|", "\\|")
        lines.append(f"| {name.replace('_', ' ')} | {r.status} | {headline} |")
    lines.append("")

    # Straight from the tools, bypassing the model entirely. "Costs are within
    # thresholds" with no figure is not a useful sentence, and no amount of
    # prompting reliably stops a model writing it.
    if key_numbers := _key_numbers():
        lines += ["## Key numbers", "", "```", key_numbers, "```", ""]

    for name, r in {**reports, "code": code}.items():
        lines.append(f"## {name.replace('_', ' ').title()}")
        lines.append("")
        lines.append(f"_{r.headline}_")
        lines.append("")
        if r.facts:
            lines += ["```", *r.facts, "```", ""]
        lines.append(_render_findings(r.findings))

    if code.patches:
        lines += ["## Proposed patches", ""]
        lines.append(
            "These are proposals only. Nothing has been applied. Each is written "
            "to `reports/patches/` and, when the workflow runs with pull-request "
            "permissions, opened as a draft PR for review."
        )
        lines.append("")
        for i, p in enumerate(code.patches, 1):
            lines += [
                f"### {i}. `{p.file_path}` (risk: {p.risk})",
                "",
                p.rationale,
                "",
            ]
            if p.diff:
                lines += ["```diff", p.diff.strip(), "```", ""]
            else:
                lines += ["_No diff produced; see rationale above._", ""]

    lines += ["---", ""]
    # The trace is how you check the agent's working. Every number above came
    # from a tool call, and this link is where you can see which one.
    if trace_url:
        lines += [f"[Full LangSmith trace for this run]({trace_url})", ""]
    lines += [
        f"Generated by the TransitPulse ops agent at "
        f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')} "
        f"using `{config.GROQ_MODEL}` via Groq. "
        f"Thresholds: {config.DAILY_COST_THRESHOLD:.2f} USD/day, "
        f"{config.MONTHLY_COST_THRESHOLD:.2f} USD/month, "
        f"{config.TARGET_COLLECTION_DAYS} day collection target.",
        "",
    ]
    return "\n".join(lines)


def write_outputs(report_md: str, code: CodeReport) -> tuple[str, list[str]]:
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = config.REPORTS_DIR / f"{config.run_date()}.md"
    report_path.write_text(report_md, encoding="utf-8")

    patch_paths: list[str] = []
    if config.PROPOSE_PATCHES and code.patches:
        config.PATCHES_DIR.mkdir(parents=True, exist_ok=True)
        for i, p in enumerate(code.patches, 1):
            if not p.diff.strip():
                continue
            slug = p.file_path.replace("/", "_").replace(".", "_")
            path = config.PATCHES_DIR / f"{config.run_date()}-{i:02d}-{slug}.patch"
            path.write_text(p.diff.rstrip() + "\n", encoding="utf-8")
            patch_paths.append(str(path.relative_to(config.REPO_ROOT)))

    return str(report_path.relative_to(config.REPO_ROOT)), patch_paths


def _emit_ci_outputs(result: dict) -> None:
    """Hand the workflow what it needs to decide whether to open a PR or shout."""
    if out := os.getenv("GITHUB_OUTPUT"):
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"status={result['status']}\n")
            fh.write(f"critical={result['critical']}\n")
            fh.write(f"warning={result['warning']}\n")
            fh.write(f"report_path={result['report_path']}\n")
            fh.write(f"has_patches={'true' if result['patches'] else 'false'}\n")
            fh.write(f"trace_url={result['trace_url']}\n")
    if summary_file := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(summary_file, "a", encoding="utf-8") as fh:
            fh.write(f"### TransitPulse agent: {result['status']}\n\n")
            fh.write(
                f"{result['critical']} critical, {result['warning']} warning. "
                f"Report: `{result['report_path']}`\n"
            )
            if result["trace_url"]:
                fh.write(f"\n[View the LangSmith trace]({result['trace_url']})\n")


# The root span. Everything the agents do nests underneath this one run, so a
# day's work is a single trace in LangSmith rather than four unrelated ones.
# The returned dict becomes the root span's output, which means the day's
# verdict is visible at the top of the trace without expanding anything.
@tracing.traced_run("transitpulse-daily-report")
def run_daily_report() -> dict:
    subagents = build_subagents()
    reports: dict[str, SubagentReport] = {}
    for name, agent in subagents.items():
        reports[name] = run_subagent(name, agent, _TASKS[name])
        log.info("  %s -> %s", name, reports[name].status)

    code_result = run_subagent("code", build_code_agent(), _code_task(reports), CodeReport)
    # run_subagent returns the schema the agent was built with, but a failure
    # path returns the base class. Normalising here keeps build_report simple.
    code = (
        code_result
        if isinstance(code_result, CodeReport)
        else CodeReport(**code_result.model_dump(), patches=[])
    )

    summary = _summary(reports, code)

    # Resolved inside the traced function -- outside it there is no current run
    # tree and the URL would come back empty.
    trace_url = tracing.current_trace_url()

    report_md = build_report(reports, code, summary, trace_url)
    report_path, patches = write_outputs(report_md, code)

    counts = _severity_counts({**reports, "code": code})
    return {
        "status": _overall_status({**reports, "code": code}),
        "critical": counts["critical"],
        "warning": counts["warning"],
        "info": counts["info"],
        "report_path": report_path,
        "patches": patches,
        "trace_url": trace_url,
        "headlines": {n: r.headline for n, r in {**reports, "code": code}.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TransitPulse daily ops agent")
    parser.add_argument(
        "--tools-only",
        action="store_true",
        help=(
            "Call every tool directly and print the raw output, with no model in "
            "the loop. Use this to verify AWS permissions without spending Groq "
            "tokens."
        ),
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Exit non-zero when a critical finding is present.",
    )
    parser.add_argument(
        "--no-trace",
        action="store_true",
        help="Disable LangSmith tracing even when LANGSMITH_API_KEY is set.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
    )

    if args.tools_only:
        return _tools_only()

    # Before any agent is built: LangChain reads the tracing environment at call
    # time, and the agents are constructed with a model client that captures it.
    if args.no_trace:
        tracing.disable()
    else:
        tracing.configure()

    # Fail before writing anything. A report saying only "the model is blocked"
    # four times is worse than no report: it gets committed to main, it reads as
    # a pipeline incident, and it buries the one line that matters. A red
    # workflow with this message in the annotation is the honest signal.
    if reason := verify_model():
        log.error("cannot start: %s", reason)
        if os.getenv("GITHUB_ACTIONS"):
            print(f"::error title=Agent model unavailable::{reason.splitlines()[0]}")
        return 2

    try:
        result = run_daily_report()
    finally:
        # In a finally block on purpose. The run that crashed is the one whose
        # trace you most want, and a background flush that never happens because
        # the process died is how you lose it.
        tracing.flush()

    _emit_ci_outputs(result)

    log.info(
        "wrote %s (status=%s, %d critical, %d warning)",
        result["report_path"],
        result["status"],
        result["critical"],
        result["warning"],
    )
    if result["patches"]:
        log.info(
            "wrote %d patch proposal(s): %s",
            len(result["patches"]),
            ", ".join(result["patches"]),
        )
    if result["trace_url"]:
        log.info("trace: %s", result["trace_url"])

    if args.fail_on_critical and result["critical"]:
        return 1
    return 0


def _tools_only() -> int:
    """Exercise every tool with no model involved.

    This is the first thing to run after deploying the IAM role: it surfaces
    every missing permission in one pass, and costs nothing.
    """
    from agent.tools import COST_TOOLS, DATA_TOOLS, HEALTH_TOOLS, REPO_TOOLS

    failures = 0
    for group_name, tools in (
        ("health", HEALTH_TOOLS),
        ("cost", COST_TOOLS),
        ("data", DATA_TOOLS),
        ("repo", REPO_TOOLS),
    ):
        for t in tools:
            # These need an argument the caller has to choose, so a smoke test
            # cannot exercise them meaningfully.
            if t.name in ("read_error_logs", "athena_query", "read_source_file", "search_source"):
                continue
            print(f"\n{'=' * 70}\n{group_name}.{t.name}\n{'=' * 70}")
            try:
                out = t.invoke({})
                print(out)
                if "TOOL_UNAVAILABLE" in out or "TOOL_ERROR" in out:
                    failures += 1
            except Exception as exc:
                print(f"RAISED: {type(exc).__name__}: {exc}")
                failures += 1

    print(f"\n{'=' * 70}\n{failures} tool(s) could not complete.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
