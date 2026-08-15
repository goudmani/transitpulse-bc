"""One shared ChatGroq factory, paced to fit inside the free tier.

A five-agent run is nowhere near the requests-per-minute ceiling but walks
straight into the *tokens*-per-minute one, because every agent turn resends the
whole conversation including all prior tool output. Three things keep it inside:

1. A rate limiter sized from the model's own TPM budget, so switching models
   re-paces the run instead of silently over-driving the new limit.
2. Tools that return small, pre-summarised payloads rather than raw boto3
   responses -- see the note at the top of tools/_aws.py.
3. `reasoning_effort="low"` on reasoning models, because thinking tokens bill
   against the same budget as the answer.
"""

from __future__ import annotations

import logging
import os

from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_groq import ChatGroq

from agent import config

log = logging.getLogger(__name__)

# Groq free-tier tokens per minute, per model. TPM is the binding constraint
# here, not RPM: at 30 RPM and 8K TPM you get roughly 260 tokens per request on
# average, and a single agent turn carrying four tool results is far past that.
_FREE_TIER_TPM = {
    "openai/gpt-oss-120b": 8_000,
    "openai/gpt-oss-20b": 8_000,
    "llama-3.3-70b-versatile": 12_000,
    "llama-3.1-8b-instant": 6_000,
}
_DEFAULT_TPM = 8_000

# Rough average tokens per request across a run: a system prompt of ~700, plus a
# transcript that grows with each tool result. Sized against observed runs, which
# completed comfortably at 0.25 rps against a 12K TPM budget -- that implies a
# real average well under 1K, so 1.5K is already cautious. It only has to be the
# right order of magnitude: it sets the pacing, and the 429 retry with
# Retry-After absorbs the tail where late-run transcripts are largest.
_EST_TOKENS_PER_REQUEST = 1_500

# Reasoning models charge for thinking tokens too. Groq does not report them
# separately in a way worth parsing, so pace as if they add half again.
_REASONING_OVERHEAD = 1.5

_REASONING_MODELS = ("gpt-oss", "qwen")


def _is_reasoning_model(model: str) -> bool:
    return any(m in model.lower() for m in _REASONING_MODELS)


def _requests_per_second(model: str) -> float:
    """Pace derived from the model's TPM budget rather than hardcoded.

    The previous fixed 0.25 rps was sized for llama's 12K TPM. Moving to
    gpt-oss-120b drops the budget to 8K without changing anything visible, so a
    hardcoded rate quietly becomes a 429 loop that only shows up as a slow run.
    """
    if override := os.getenv("AGENT_RPS"):
        return float(override)

    tpm = _FREE_TIER_TPM.get(model, _DEFAULT_TPM)
    per_request = _EST_TOKENS_PER_REQUEST
    if _is_reasoning_model(model):
        per_request *= _REASONING_OVERHEAD

    rps = tpm / 60.0 / per_request
    # Floors and ceilings so an unknown model cannot stall the run entirely or
    # let it stampede. 0.03 rps is one request per ~33s; a ~20-call run is then
    # ~11 minutes, inside the workflow's 25-minute timeout.
    return max(0.03, min(rps, 0.5))


_LIMITER = InMemoryRateLimiter(
    requests_per_second=_requests_per_second(config.GROQ_MODEL),
    check_every_n_seconds=0.1,
    # Allows a short burst after an idle stretch (an agent thinking between tool
    # calls) without letting a whole run fire at once.
    max_bucket_size=3,
)


def build_llm(
    temperature: float | None = None,
    reasoning_effort: str | None = None,
) -> ChatGroq:
    """A ChatGroq client.

    Every client built here shares the one module-level `_LIMITER` object, which
    is what makes the pacing real: `InMemoryRateLimiter` meters per instance, so
    clients holding *separate* limiters would each get a full budget and the
    limiting would do nothing. Sharing the limiter is the requirement; sharing
    the client is not, which is what lets the code agent run at a different
    reasoning effort without breaking the budget.
    """
    if not os.getenv("GROQ_API_KEY"):
        raise RuntimeError(
            "GROQ_API_KEY is not set. Locally: put it in .env at the repo root "
            "(loaded by agent/__init__.py) or export it. "
            "In CI: add it under Settings -> Secrets and variables -> Actions."
        )

    kwargs = {}
    if _is_reasoning_model(config.GROQ_MODEL):
        kwargs["reasoning_effort"] = reasoning_effort or config.REASONING_EFFORT
        # Keep the chain-of-thought out of message content. Left in, it lands in
        # the transcript, gets resent on every subsequent turn, and can confuse
        # the structured-output parser into reading reasoning as the answer.
        # gpt-oss uses include_reasoning; other families use reasoning_format,
        # and the two are mutually exclusive.
        if "gpt-oss" in config.GROQ_MODEL.lower():
            kwargs["model_kwargs"] = {"include_reasoning": False}
        else:
            kwargs["reasoning_format"] = "hidden"

    log.info(
        "model=%s rps=%.3f%s",
        config.GROQ_MODEL,
        _LIMITER.requests_per_second,
        f" reasoning_effort={kwargs['reasoning_effort']}" if "reasoning_effort" in kwargs else "",
    )

    return ChatGroq(
        model=config.GROQ_MODEL,
        temperature=config.GROQ_TEMPERATURE if temperature is None else temperature,
        timeout=config.LLM_TIMEOUT_SECONDS,
        # Groq returns 429 with a Retry-After on a token-per-minute overrun. The
        # SDK honours it, so retries are the difference between a paced run and
        # a failed one.
        max_retries=4,
        rate_limiter=_LIMITER,
        **kwargs,
    )


_SHARED: ChatGroq | None = None
_CODE: ChatGroq | None = None


def shared_llm() -> ChatGroq:
    """For the infrastructure, cost and data agents, and the supervisor summary.

    Low reasoning effort: their job is to read a tool's output and quote it, and
    the arithmetic that could be got wrong is done in Python.
    """
    global _SHARED
    if _SHARED is None:
        _SHARED = build_llm()
    return _SHARED


def code_llm() -> ChatGroq:
    """For the code agent only, at higher reasoning effort.

    Tracing a failure to a source line and writing a diff that applies is the
    one task here that is genuinely multi-step, and it is also the cheapest
    place to spend the extra tokens: the code agent runs once, and on a healthy
    day it returns in a single turn having found nothing to do.

    Shares `_LIMITER` with shared_llm(), so the extra effort costs tokens but
    not rate-limit headroom.
    """
    global _CODE
    if _CODE is None:
        _CODE = build_llm(reasoning_effort=config.CODE_REASONING_EFFORT)
    return _CODE
