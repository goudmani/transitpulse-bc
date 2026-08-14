"""TransitPulse ops agent: a supervisor plus four specialist subagents that run
once a day, inspect the running pipeline, and write a report to reports/.

Entry point: `python -m agent.supervisor`.
"""

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    # A local convenience, not a requirement. In CI the secrets arrive as real
    # environment variables and there is no .env to read, so a missing dotenv
    # must not be able to stop the agent running.
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False


# Loaded here, in the package __init__, because agent.config reads the
# environment at import time to build its module-level constants. Anywhere later
# -- in main(), or at the top of config.py -- and the constants would already be
# frozen against an environment that had not been populated yet.
#
# override=False is the important argument: real environment variables win over
# the file. In CI the secrets arrive as real env vars and there is no .env at
# all, so this is a no-op there; locally it means `GROQ_API_KEY=... make agent`
# still beats whatever .env says, which is what you want when testing a second
# key.
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
