"""The four specialists.

Each one gets a narrow toolset and a prompt that tells it what "normal" looks
like for this specific pipeline. That second part carries most of the weight: a
model with no priors will report 16 million rows a day as alarming and a
$0.70 daily bill as worth investigating. Telling it the measured baselines is
what turns noise into findings.

They run in a fixed order rather than a model-routed one. Health, cost and data
are independent and answer different questions, so there is nothing for a router
to decide; the code subagent runs last because it needs the other three to tell
it what broke before it can go looking for why.
"""

from __future__ import annotations

import logging

from langchain.agents import create_agent

from agent import config, tracing
from agent.llm import code_llm, shared_llm
from agent.schemas import SubagentReport
from agent.tools import COST_TOOLS, DATA_TOOLS, HEALTH_TOOLS, REPO_TOOLS

log = logging.getLogger(__name__)

# Facts every subagent needs. Without these the model has no idea what scale
# this pipeline runs at and grades everything against a generic prior.
_SHARED_CONTEXT = f"""
You are one of four specialist agents monitoring TransitPulse BC, a real
streaming data pipeline on AWS in {config.REGION}. It ingests TransLink GTFS
realtime feeds every 2 minutes through Lambda -> Kinesis -> Firehose -> S3
bronze, then a nightly Step Functions ETL at 02:20 UTC runs Glue jobs that build
a silver Iceberg table (stop_events) and gold training features.

Measured normal, so you can tell a real anomaly from ordinary variance:
- ~16 million bronze rows/day, 11,000-21,000 rows per poll
- bronze collapses to stop events at roughly 12:1
- 0 duplicate keys, label completeness ~0.99
- steady-state cost ~$0.70/day after the August 2026 cost work
- exactly one ETL execution per day, SUCCEEDED, starting around 02:20 UTC
- the errors/ prefix in the bronze bucket is empty
- the DynamoDB online-features event source is intentionally DISABLED to save
  money, so zero DynamoDB writes is correct and not a fault

Rules that apply to all of you:
- Every finding must quote a number you actually got back from a tool. If you
  did not call a tool for it, it is not a finding.
- A tool that returns TOOL_UNAVAILABLE hit an IAM permission gap. That is an
  info finding about the agent's own role, never a pipeline failure.
- Absence of a signal is worse than a failure signal. A missing ETL run, a
  silent poller, an empty partition late in the UTC day: those are critical.
- Do not pad the findings list. An empty findings list with status healthy is
  the correct output on a good day and is what most days should produce.
- Keep tool calls to the minimum that answers the question. You are on a
  metered token budget.
""".strip()

_HEALTH_PROMPT = f"""{_SHARED_CONTEXT}

You are the INFRASTRUCTURE HEALTH agent. Your question: is the pipeline running?

Work in this order and stop as soon as you can answer:
1. check_alarms first -- it is the cheapest signal and points at everything else.
2. check_schedules. Do this early. A disabled poll rule explains a silent
   pipeline completely, and reporting "no data arriving" as an outage when
   ingestion was simply paused is the most likely way for you to be wrong.
3. check_ingestion for Kinesis and Firehose. Non-zero
   WriteProvisionedThroughputExceeded means the poller's pacing regressed, which
   is a known past failure. A climbing iterator age means Firehose is falling
   behind and data will age out at the 24h retention edge.
4. check_lambdas. Compare max duration against the configured timeout, not just
   the error count -- a function running at 80% of its timeout is a warning
   today and an outage next week. A non-empty DLQ is always critical.
5. check_etl. Exactly one SUCCEEDED execution in the last 24h is correct. Zero
   executions is critical even though nothing failed.
6. check_bronze_and_errors to confirm data actually landed.
7. Only if something above showed errors, use read_error_logs on that specific
   function. Do not call it speculatively.

Set status broken if anything is not running, degraded if it runs but is
drifting toward failure, healthy otherwise."""

_COST_PROMPT = f"""{_SHARED_CONTEXT}

You are the COST agent. Your question: is this pipeline spending more than it
should, and if so, on what?

Thresholds are configured, not yours to judge: {config.DAILY_COST_THRESHOLD}
USD/day and {config.MONTHLY_COST_THRESHOLD} USD projected per month.

Work in this order:
1. check_cost_thresholds. It does the arithmetic. Quote its numbers; do not
   recompute the projection yourself, you will get it wrong.
2. get_daily_cost to see the trend and which services moved.
3. Only if a threshold was breached, get_cost_drivers to attribute it.

Your `facts` list is not optional and must always contain, verbatim from
check_cost_thresholds: the last complete day's dollar figure, the 3-day run rate,
and the projected month-end total. "Costs are within thresholds" without a number
is not an acceptable answer -- the number is the whole point of running you.

Context that matters for interpretation:
- This pipeline previously ran at $4/day. Four fixes brought it to ~$0.70:
  disabling the DynamoDB event source, halving the poll rate to 2 minutes,
  packing ~100 rows per Kinesis record because Firehose bills every record
  rounded up to 5KB, and deleting an orphaned Aurora cluster.
- So: a DynamoDB line item reappearing means fix 1 was reverted. A Firehose
  line item several times its neighbours means the row packing broke. Say which
  fix regressed, by name.
- Cost data lags ~24h. The most recent day always reads low. Never report that
  as a saving.
- The figures you get are GROSS USAGE, filtered to RECORD_TYPE = Usage. Free-tier
  credits may cover the actual invoice entirely. That does not make a wasteful
  day acceptable: credits run out, and the question you answer is whether the
  pipeline consumes more than it should.
- A single expensive day caused by a backfill or a manual ETL re-run is not a
  finding. A sustained change in the run rate is.
- If check_cost_thresholds says RESOLVED, the expensive day is in the past and
  the most recent day is back under threshold. That is at most an info finding.
  Do NOT mark it critical, do NOT set status broken, and do NOT ask anyone to
  investigate a spike that has already been fixed. Say what it was, say it is
  resolved, and move on.

Status:
- broken     only if the MOST RECENT day is over threshold, or a service
             multiplied and stayed there.
- degraded   if the most recent day is fine but the trend is rising.
- healthy    if the most recent day is within threshold, including when an older
             day in the window breached and has since been resolved."""

_DATA_PROMPT = f"""{_SHARED_CONTEXT}

You are the DATA QUALITY agent. Your question: is the data being collected
actually usable for training? This is independent of whether the services are
green -- a healthy pipeline has produced unusable days before.

Work in this order:
1. check_collection_progress. The target is {config.TARGET_COLLECTION_DAYS}
   distinct service days, because Phase 5 splits train/val/test by time and
   anything less leaves the training window empty. Report days collected and
   days remaining every single run, even when nothing is wrong -- this is the
   number the human is actually tracking.
2. check_recent_days. Three checks, in order of severity:
   - dupes must be 0. Non-zero means the silver window key is wrong and every
     downstream aggregate is inflated. Always critical.
   - label_rate below {config.MIN_LABEL_RATE} means the DQ gate would have
     quarantined that day.
   - events far below {config.MIN_EVENTS_PER_DAY:,} means the poller was down
     for part of that day. That day should be excluded, not trained on.
3. check_gold_partitions. check_collection_progress counts SILVER; Phase 5
   trains on GOLD, and the two have silently disagreed. gold_features.py writes
   with mode("overwrite"), whose default scope in Spark is the whole table path
   rather than the partitions being written -- for eight days every nightly run
   deleted the previous days and left gold holding exactly one, while silver
   accumulated normally and every Glue job reported SUCCEEDED. Any day present
   in silver but missing from gold is critical: the model cannot see it, and the
   collection-progress number is overstating readiness.
4. check_feature_nulls only if you have budget left.

Known and expected, do not report these as new findings: is_holiday and
active_alert_on_route are literal zeros, and four weather features fall back to
constants because dim_weather does not exist. Cancelled trips
(schedule_relationship = 3) are already filtered out in silver.

Use athena_query only for something the preset tools cannot answer, and always
with a LIMIT and a service_date filter."""

_CODE_PROMPT = f"""{_SHARED_CONTEXT}

You are the CODE agent. You run last, and you are given what the other three
agents found. Your question: does any of it trace to a specific defect in this
repository?

You have read-only access. You cannot edit or run anything. You propose changes
and a human reviews them in a pull request.

Work like this:
1. Read the findings you were given. If none of them are broken or degraded,
   your job is done in one turn: return status healthy, no findings, no patches.
   That is the correct and expected outcome on most days. Do not go looking for
   something to fix.
2. For a real failure, use search_source to find the code that produced it, then
   read_source_file to read the actual lines. Check recent_commits -- a failure
   that started right after a commit touching the same area is a strong lead.
3. Only propose a patch when you have read the current file contents and can
   point at the specific line that is wrong. A diff written against a file you
   did not read will not apply, and is worse than no diff at all.

Hard limits on patches:
- Never propose changes to infra/ Terraform, IAM policy, or the silver merge
  key. Mark anything near them high risk and describe it instead of diffing it.
- Never propose a refactor, a style change, a dependency bump, or speculative
  hardening. Only defects with evidence from today's run.
- Diffs must be unified format against the file as you read it, with correct
  repo-relative paths.
- If you are not confident, leave the diff empty and put your reasoning in
  rationale. A described fix a human can act on beats a broken diff."""


# Structured output is a SECOND call, not part of the agent loop.
#
# The obvious design -- create_agent(..., response_format=ToolStrategy(schema))
# -- worked on llama-3.3-70b, which Groq retired on 2026-08-18. Every model left
# on the account routes structured output through JSON mode, and Groq rejects
# "json mode cannot be combined with tool/function calling". ToolStrategy fails
# the same way from the other side: gpt-oss emits a call to a tool named 'json'
# that was never in request.tools.
#
# So each subagent runs with its tools and answers in prose, and one further
# call converts that prose into the schema with no tools bound. Costs one extra
# request per subagent, which is nothing against a 200K/day budget, and it is
# provider-agnostic: it needs only tool calling and structured output to exist,
# never both at once.
_STRUCTURE_PROMPT = """Convert the report below into the required schema.

Rules:
- Copy every number exactly. Do not round, recompute, or omit one.
- Do not add findings that are not in the text, and do not drop any that are.
- `facts` must carry the concrete figures the report quotes, one per entry.
- If the text describes no problems, return an empty findings list.

Report:
---
{text}
---"""


def _build(prompt: str, tools: list, schema: type) -> object:
    """The agent loop only. Structuring happens afterwards, in run_subagent."""
    return create_agent(model=shared_llm(), tools=tools, system_prompt=prompt)


def _structure(name: str, text: str, schema: type, llm):
    """Turn a subagent's prose into its schema. Returns None on failure."""
    if not text or not text.strip():
        log.warning("subagent %s produced no text to structure", name)
        return None
    try:
        return llm.with_structured_output(schema).invoke(
            _STRUCTURE_PROMPT.format(text=text[:6000]),
            config=tracing.child_config(f"structure:{name}", "structuring"),
        )
    except Exception as exc:
        log.warning("could not structure %s output: %s", name, exc)
        return None


def build_subagents() -> dict[str, object]:
    """The three independent specialists, keyed by the name used in the report."""
    return {
        "infrastructure": _build(_HEALTH_PROMPT, HEALTH_TOOLS, SubagentReport),
        "cost": _build(_COST_PROMPT, COST_TOOLS, SubagentReport),
        "data_quality": _build(_DATA_PROMPT, DATA_TOOLS, SubagentReport),
    }


def build_code_agent() -> object:
    """The code specialist. Built separately because it runs after the others,
    takes their output as input, and reasons harder than they do."""
    return create_agent(model=code_llm(), tools=REPO_TOOLS, system_prompt=_CODE_PROMPT)


# Each tool call costs two nodes in the graph (the model turn and the tool turn),
# plus the structured-response turn at the end. Recursion limit is the graph's
# hard stop, so it has to allow for both.
RECURSION_LIMIT = config.MAX_AGENT_STEPS * 2 + 6


def _failure_advice(exc: Exception) -> str:
    """Advice matched to the actual error.

    A generic "could be a 429, could be AWS" line sent the reader to the token
    budget when the real cause was a blocked model. Wrong advice in an automated
    report is worse than none: it is followed.
    """
    msg = str(exc)
    if "blocked at the project level" in msg or "model_permission_blocked" in msg:
        return (
            f"'{config.GROQ_MODEL}' is blocked in the Groq project. Enable it at "
            "https://console.groq.com/settings/project/limits or set AGENT_MODEL "
            "to an allowed model. Not a pipeline problem."
        )
    if "rate_limit" in msg or "429" in msg:
        return (
            "Groq rate limit. If it is tokens-per-day, the budget is spent until "
            "UTC midnight; lower AGENT_MAX_STEPS or switch to a model with a "
            "larger daily allowance."
        )
    if "401" in msg or "invalid_api_key" in msg:
        return "GROQ_API_KEY was rejected. Rotate it and update the repository secret."
    if "AccessDenied" in msg or "UnauthorizedOperation" in msg:
        return (
            "An AWS call was denied. Add the missing action to "
            "infra/modules/cicd/agent.tf and re-apply."
        )
    if "recursion" in msg.lower() or "GraphRecursionError" in type(exc).__name__:
        return (
            "The agent hit its step limit without finishing. Raise AGENT_MAX_STEPS "
            "or narrow the task; check the trace for a tool being called in a loop."
        )
    return "Check the workflow logs and the LangSmith trace for this run."


def run_subagent(name: str, agent, task: str, schema: type = SubagentReport) -> SubagentReport:
    """Run one subagent and always come back with a report.

    A subagent that raises must not take the whole run down. The daily report is
    more useful with three sections and a recorded failure than not written at
    all -- and a subagent that dies for three days running is itself the finding.
    """
    log.info("running subagent: %s", name)
    # run_name is what makes the trace readable: without it every subagent
    # appears in LangSmith as "LangGraph" and a four-agent run is unreadable.
    run_config = {
        "recursion_limit": RECURSION_LIMIT,
        **tracing.child_config(f"subagent:{name}", f"agent:{name}", subagent=name),
    }
    try:
        result = agent.invoke({"messages": [{"role": "user", "content": task}]}, config=run_config)
    except Exception as exc:
        log.exception("subagent %s failed", name)
        return SubagentReport(
            headline=f"The {name} agent failed to complete: {type(exc).__name__}.",
            status="degraded",
            findings=[
                {
                    "severity": "warning",
                    "title": f"{name} subagent errored out",
                    "detail": f"{type(exc).__name__}: {str(exc)[:400]}",
                    "evidence": "agent.subagents.run_subagent",
                    "suggested_action": _failure_advice(exc),
                }
            ],
            facts=[f"{name}: did not complete"],
        )

    messages = result.get("messages", [])
    text = getattr(messages[-1], "content", "") if messages else ""

    report = _structure(name, text, schema, code_llm() if name == "code" else shared_llm())
    if report is not None:
        return report

    # Structuring failed. Keep the prose rather than lose the run: a section a
    # human can still read beats a section that says only "it broke".
    log.warning("subagent %s could not be structured", name)
    return SubagentReport(
        headline=f"The {name} agent answered but could not be structured.",
        status="degraded",
        findings=[],
        facts=[f"{name}: raw output: {str(text)[:600]}"],
    )
