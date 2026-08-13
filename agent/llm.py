"""One shared ChatGroq factory, paced to fit inside the free tier.

The free tier allows 30 requests/minute, 12K tokens/minute and 100K tokens/day
on llama-3.3-70b-versatile. A five-agent run is nowhere near the request limit
but can walk into the *token* limit, because every agent turn resends the whole
conversation including tool output. Two things keep it inside:

1. The rate limiter below, which spaces requests out so a burst of tool calls
   cannot stack several large prompts into the same minute.
2. Tools that return small, pre-summarised payloads rather than raw boto3
   responses -- see the note at the top of tools_aws.py.

`requests_per_second` is deliberately well under the 30 RPM ceiling. The binding
constraint is tokens per minute, not requests, and a request carrying a 4K-token
transcript costs a third of the minute's budget on its own.
"""

from __future__ import annotations

import os

from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_groq import ChatGroq

from agent import config

# 0.25 rps = one request every 4 seconds. max_bucket_size=3 allows a short burst
# after an idle stretch (an agent thinking between tool calls) without letting a
# whole run fire at once.
_LIMITER = InMemoryRateLimiter(
    requests_per_second=float(os.getenv("AGENT_RPS", "0.25")),
    check_every_n_seconds=0.1,
    max_bucket_size=3,
)


def build_llm(temperature: float | None = None) -> ChatGroq:
    """A ChatGroq client shared by every agent in the run.

    Sharing one instance matters: `InMemoryRateLimiter` paces per instance, so a
    separate client per subagent would give each its own budget and defeat the
    limiting entirely.
    """
    if not os.getenv("GROQ_API_KEY"):
        raise RuntimeError(
            "GROQ_API_KEY is not set. Locally: put it in .env at the repo root "
            "(loaded by agent/__init__.py) or export it. "
            "In CI: add it under Settings -> Secrets and variables -> Actions."
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
    )


_SHARED: ChatGroq | None = None


def shared_llm() -> ChatGroq:
    """Process-wide singleton, so all agents share one rate-limit bucket."""
    global _SHARED
    if _SHARED is None:
        _SHARED = build_llm()
    return _SHARED
