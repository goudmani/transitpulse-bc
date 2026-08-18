"""Every tunable in one place, all overridable by environment variable.

The workflow sets these as `env:` entries, so changing a threshold is a one-line
edit to the YAML rather than a code change, and running the agent locally against
a different account or a different budget needs no edit at all.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = REPO_ROOT / "reports"
PATCHES_DIR = REPORTS_DIR / "patches"

# --- AWS -------------------------------------------------------------------

REGION = os.getenv("AWS_REGION", "ca-central-1")

# Cost Explorer only answers on the global endpoint, which lives in us-east-1
# regardless of where the resources are. Querying it in ca-central-1 returns an
# empty result set rather than an error, which reads as "$0 spent".
CE_REGION = "us-east-1"

# Every resource this project owns is named `transitpulse-*`, so the tools
# discover resources by prefix instead of carrying a hardcoded inventory that
# would silently go stale the next time a module is renamed.
RESOURCE_PREFIX = os.getenv("RESOURCE_PREFIX", "transitpulse")

GLUE_DATABASE = os.getenv("GLUE_DATABASE", "transitpulse")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "transitpulse")

# --- cost thresholds -------------------------------------------------------

# Measured steady state after the 2026-08-12 cost work is ~$0.70/day. $1.50
# leaves room for a backfill or a manual ETL re-run without crying wolf, and
# still fires long before the $4/day the pipeline was burning before the fix.
DAILY_COST_THRESHOLD = float(os.getenv("DAILY_COST_THRESHOLD", "1.50"))

# Month-end projection is the number that actually matters for a budget: a
# single $3 day is noise, a $3/day trend is a $90 month.
MONTHLY_COST_THRESHOLD = float(os.getenv("MONTHLY_COST_THRESHOLD", "45.00"))

# Any single service moving more than this much day over day gets called out by
# name, which is how the DynamoDB and Firehose overruns were found the first time.
SERVICE_DELTA_THRESHOLD = float(os.getenv("SERVICE_DELTA_THRESHOLD", "0.25"))

# Cost allocation tags have to be activated in the Billing console and do not
# backfill. Until that is done a tag filter matches nothing and returns $0, so
# the default is an unfiltered account-wide query. Flip this on once the
# `Project` tag shows up as active in Billing -> Cost allocation tags.
COST_FILTER_BY_TAG = os.getenv("COST_FILTER_BY_TAG", "false").lower() == "true"

# --- data quality thresholds ----------------------------------------------

# Phase 5 splits train/val/test by time (<=T-14d, T-14d..T-7d, >T-7d), so
# anything under 21 days leaves the training window empty.
TARGET_COLLECTION_DAYS = int(os.getenv("TARGET_COLLECTION_DAYS", "21"))

# Below this the DQ gate would have quarantined the day anyway.
MIN_LABEL_RATE = float(os.getenv("MIN_LABEL_RATE", "0.85"))

# A day at a fraction of normal volume means the poller was down for part of it.
# Better to exclude that day than to train on it.
MIN_EVENTS_PER_DAY = int(os.getenv("MIN_EVENTS_PER_DAY", "150000"))

# --- model -----------------------------------------------------------------

# openai/gpt-oss-120b. Not chosen so much as left standing.
#
# Groq RETIRED llama-3.3-70b-versatile on 2026-08-18, mid-project. The catalog
# went from 15 models to 13 overnight and the nightly run failed with a bare
# 404. Nothing in this repo changed; the provider deprecated the model. Of what
# remains, qwen/qwen3.6-27b and gpt-oss-20b are blocked at the organization
# level, so gpt-oss-120b is the only usable chat model on this account.
#
#   openai/gpt-oss-120b   30 RPM, 8K TPM, 200K TPD, reasoning model
#
# It reasons and reads code better than llama did, and its daily budget is
# double. The cost is that it forced the two-step structuring in subagents.py:
# gpt-oss routes structured output through JSON mode, and Groq rejects "json
# mode cannot be combined with tool/function calling", so a subagent cannot
# hold tools and a response schema in the same call. See the comment above
# _build() for what that changed.
#
# Lesson worth keeping: a hosted model is a dependency that can be withdrawn
# without notice, and pinning one in config is not the same as controlling it.
GROQ_MODEL = os.getenv("AGENT_MODEL", "openai/gpt-oss-120b")
GROQ_TEMPERATURE = float(os.getenv("AGENT_TEMPERATURE", "0.1"))

# Reasoning models emit thinking tokens that bill against the same TPM/TPD
# budget as the answer, so effort is spent where it changes the answer.
#
# "low" for the infrastructure, cost and data agents. Their work is reading a
# tool's output table and quoting numbers from it; the threshold arithmetic and
# the month-end projection are done in Python precisely so a model never has to
# derive them. More thinking does not make a quoted number more correct, and at
# 8K TPM the tokens are better spent on tool output.
#
# Higher for the code agent, which is the one place multi-step deduction is the
# actual task: correlate a failure with a source line, check it against recent
# commits, and produce a diff that applies. Low effort there shows up as shallow
# diagnoses and diffs that do not apply -- not as invented facts.
#
# Neither setting is a hallucination control. Grounding is: every finding must
# quote a tool result, and the supervisor copies findings verbatim rather than
# re-narrating them. Both are ignored for non-reasoning models.
REASONING_EFFORT = os.getenv("AGENT_REASONING_EFFORT", "low")
CODE_REASONING_EFFORT = os.getenv("AGENT_CODE_REASONING_EFFORT", "medium")

# A wedged tool call should fail the step, not hang the workflow for six hours.
LLM_TIMEOUT_SECONDS = int(os.getenv("AGENT_LLM_TIMEOUT", "120"))
MAX_AGENT_STEPS = int(os.getenv("AGENT_MAX_STEPS", "12"))

# --- behaviour flags -------------------------------------------------------

# The code subagent proposes patches; it never applies them. When this is on it
# writes unified diffs to reports/patches/ for the workflow to open a draft PR
# from. Nothing in this package ever writes to a tracked source file.
PROPOSE_PATCHES = os.getenv("PROPOSE_PATCHES", "true").lower() == "true"

# How far back the health and log tools look. 24h lines up with a daily cadence:
# a full day of metrics, no overlap, no gaps.
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))


def run_date() -> str:
    """The date the report is filed under, in UTC.

    UTC rather than local time because every timestamp the pipeline produces
    (`service_date`, the 02:20 ETL schedule, the S3 `dt=` partitions) is UTC.
    A local-time report date would disagree with the partition it describes for
    part of every day.
    """
    return datetime.now(UTC).strftime("%Y-%m-%d")


def lookback_window() -> tuple[datetime, datetime]:
    """(start, end) for metric queries, as timezone-aware UTC datetimes."""
    end = datetime.now(UTC)
    return end - timedelta(hours=LOOKBACK_HOURS), end
