"""The contract between the subagents and the supervisor.

Subagents return one of these instead of prose. Two reasons: the supervisor can
sort and count findings without asking a model to do it, and a structured
response is far cheaper in tokens than a subagent narrating what it found --
which matters on a 12K tokens-per-minute budget.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

Severity = Literal["critical", "warning", "info"]


class Finding(BaseModel):
    """One thing worth a human's attention."""

    severity: Severity = Field(
        description=(
            "critical = the pipeline is losing data or money right now and today "
            "is compromised. warning = a trend that will become critical if "
            "ignored. info = worth recording, no action needed."
        )
    )
    title: str = Field(description="One line, under 80 characters, specific.")
    detail: str = Field(
        description=(
            "Two to four sentences. Quote the actual numbers from the tool "
            "output -- metric values, dollar amounts, row counts. No numbers "
            "means no finding."
        )
    )
    evidence: str = Field(
        default="",
        description="Which tool call and which value this came from.",
    )
    suggested_action: str = Field(
        default="",
        description="The concrete next step, ideally a command or a file to open.",
    )


class SubagentReport(BaseModel):
    """What one subagent hands back to the supervisor."""

    headline: str = Field(
        description="One sentence a human could read alone and know if today is fine."
    )
    status: Literal["healthy", "degraded", "broken"] = Field(
        description=(
            "healthy = nothing to do. degraded = works but is drifting or "
            "wasting money. broken = something is not running."
        )
    )
    findings: list[Finding] = Field(
        default_factory=list,
        description="Empty when everything checks out. Do not invent findings to fill it.",
    )
    facts: list[str] = Field(
        default_factory=list,
        description=(
            "The three to six numbers that back the status, each as "
            "'label: value'. These go into the report verbatim."
        ),
    )


class ProposedPatch(BaseModel):
    """A code change the agent thinks should be made, for a human to review."""

    file_path: str = Field(description="Repo-relative path, e.g. src/glue/gold_features.py")
    rationale: str = Field(description="Which observed failure this fixes, and how.")
    risk: Literal["low", "medium", "high"] = Field(
        description="high for anything touching Terraform, IAM, or the silver merge key."
    )
    diff: str = Field(
        default="",
        description=(
            "Unified diff against the current file. Leave empty if you could not "
            "read the file -- an unverified diff is worse than none."
        ),
    )


class CodeReport(SubagentReport):
    """The code subagent's report, plus any patches it wants to propose."""

    patches: list[ProposedPatch] = Field(
        default_factory=list,
        description=(
            "Only for defects you traced to a specific line from evidence in "
            "this run. Never propose refactors, style changes, or speculative "
            "hardening."
        ),
    )
