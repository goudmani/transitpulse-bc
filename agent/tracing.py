"""LangSmith tracing.

Every agent run becomes one trace: a root span for the day, with each subagent,
every model call and every tool call nested underneath it. That shape is the
point -- the report tells you *what* the agent concluded, and the trace tells you
*why*, which is the only way to debug a finding you think is wrong.

Tracing is entirely optional. With no LANGSMITH_API_KEY set, `traced_run` runs
the function unchanged and every helper here returns a harmless default, so a
local run needs no LangSmith account and CI does not fail when the secret is
missing. That matters more than it sounds: observability that can take down the
thing it observes is a net loss.
"""

from __future__ import annotations

import logging
import os
import subprocess
from typing import Any

from langchain_core.tracers.langchain import wait_for_all_tracers
from langsmith.run_helpers import get_current_run_tree, traceable

from agent import config

log = logging.getLogger(__name__)

DEFAULT_PROJECT = "transitpulse-ops-agent"


def is_enabled() -> bool:
    """Tracing needs both a key and the flag. A key alone is not consent."""
    return bool(os.getenv("LANGSMITH_API_KEY")) and os.getenv(
        "LANGSMITH_TRACING", "true"
    ).lower() in ("true", "1")


def configure() -> bool:
    """Set up tracing from the environment. Call once, early.

    LangChain reads these variables at call time rather than at import, so
    setting them here -- after argument parsing, before any agent is built --
    is what makes `--no-trace` work without a separate code path.
    """
    if not os.getenv("LANGSMITH_API_KEY"):
        # Explicitly off rather than merely unset: if LANGSMITH_TRACING is true
        # with no key, LangChain retries and logs a warning on every single
        # model call, which buries the actual output.
        os.environ["LANGSMITH_TRACING"] = "false"
        log.info("LangSmith tracing disabled (no LANGSMITH_API_KEY)")
        return False

    os.environ.setdefault("LANGSMITH_TRACING", "true")
    os.environ.setdefault("LANGSMITH_PROJECT", DEFAULT_PROJECT)
    log.info(
        "LangSmith tracing enabled -> project %r", os.environ.get("LANGSMITH_PROJECT")
    )
    return True


def disable() -> None:
    os.environ["LANGSMITH_TRACING"] = "false"


def _git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=config.REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def run_metadata() -> dict[str, Any]:
    """Metadata stamped on every span in the run.

    The git SHA and the run environment are the two fields that turn a trace
    from "something went wrong once" into something you can correlate: when a
    finding starts appearing every day, the SHA tells you which commit it
    started after.
    """
    return {
        "run_date": config.run_date(),
        "git_sha": _git_sha(),
        "model": config.GROQ_MODEL,
        "region": config.REGION,
        "environment": "github-actions" if os.getenv("GITHUB_ACTIONS") else "local",
        "github_run_id": os.getenv("GITHUB_RUN_ID", ""),
        "daily_cost_threshold": config.DAILY_COST_THRESHOLD,
        "monthly_cost_threshold": config.MONTHLY_COST_THRESHOLD,
    }


def run_tags(*extra: str) -> list[str]:
    """Tags to filter traces by in the LangSmith UI."""
    base = [
        "transitpulse",
        "daily-ops",
        f"model:{config.GROQ_MODEL}",
        "ci" if os.getenv("GITHUB_ACTIONS") else "local",
    ]
    return base + [t for t in extra if t]


def child_config(name: str, *tags: str, **metadata: Any) -> dict[str, Any]:
    """A RunnableConfig that names and tags one subagent's span.

    Without `run_name` every subagent shows up in LangSmith as "LangGraph",
    which makes a four-agent trace unreadable.
    """
    return {
        "run_name": name,
        "tags": run_tags(*tags),
        "metadata": {**run_metadata(), **metadata},
    }


def traced_run(name: str):
    """Decorator making the wrapped function the root span of the whole run.

    Everything invoked inside it -- agents, models, tools -- nests underneath,
    because LangChain's tracer picks up the run tree from the surrounding
    context. Without this root the four subagents would be four unrelated
    top-level traces and you would lose the one view worth having.
    """

    def decorator(fn):
        return traceable(
            run_type="chain",
            name=name,
            tags=run_tags(),
            metadata=run_metadata(),
        )(fn)

    return decorator


def current_trace_url() -> str:
    """The LangSmith URL for the run in progress, or "" if not tracing.

    Called from inside the traced function so the URL can be written into the
    report itself. A report that links to the trace behind it is the difference
    between "the agent says cost is fine" and being able to check.
    """
    if not is_enabled():
        return ""
    try:
        run = get_current_run_tree()
        return run.get_url() if run is not None else ""
    except Exception as exc:  # the URL is a nicety; never fail a run for it
        log.debug("could not resolve trace URL: %s", exc)
        return ""


def flush() -> None:
    """Wait for queued traces to finish posting.

    LangChain posts traces from a background thread. A short-lived process --
    which is exactly what a CI job is -- otherwise exits with the last spans
    still in the queue, and the run you most want to inspect is the one that
    goes missing.
    """
    if not is_enabled():
        return
    try:
        wait_for_all_tracers()
        log.info("LangSmith traces flushed")
    except Exception as exc:
        log.warning("failed to flush traces: %s", exc)
