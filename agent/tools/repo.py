"""Read-only access to this repository, for the subagent that diagnoses code.

Read-only is the whole design. The agent proposes a unified diff through its
structured response and a human reviews it in a pull request; nothing in this
package can write to a tracked source file. An agent that could edit and an
agent that could push would be one credential leak away from committing to main
at 03:00 with nobody watching.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from langchain_core.tools import tool

from agent import config

# Directories worth reasoning about. Everything else -- build artifacts, caches,
# the vendored README_files, data/ -- is noise that only costs tokens.
_ALLOWED_ROOTS = ("src", "scripts", "sql", "infra", "tests", "agent", ".github")

# Never read these, whatever the path resolution says. .env holds the TransLink
# API key; the master guide is 300KB and would exhaust the token budget in one
# call.
_DENY = re.compile(
    r"(^|/)\.env|(^|/)\.git/|\.pem$|TRANSITPULSE-MASTER-GUIDE\.md$|"
    r"(^|/)(build|data|\.terraform|__pycache__|README_files)/",
    re.IGNORECASE,
)

_MAX_READ_LINES = 200


def _resolve(rel_path: str) -> Path | None:
    """Resolve a repo-relative path, or None if it escapes the repo or is denied.

    The `is_relative_to` check is what stops `../../.ssh/id_rsa`: an agent that
    has been prompt-injected through a log line it read is exactly the scenario
    where a path traversal gets attempted.
    """
    if _DENY.search(rel_path):
        return None
    candidate = (config.REPO_ROOT / rel_path).resolve()
    if not candidate.is_relative_to(config.REPO_ROOT):
        return None
    if _DENY.search(str(candidate.relative_to(config.REPO_ROOT))):
        return None
    return candidate


@tool
def list_source_files(subdir: str = "src") -> str:
    """List the source files under a directory. Valid roots: src, scripts, sql,
    infra, tests, agent, .github.

    Use this to orient before reading anything. Reading a file you guessed the
    name of wastes a turn.
    """
    root = subdir.strip("/").split("/")[0]
    if root not in _ALLOWED_ROOTS:
        return f"REJECTED: '{root}' is not a readable root. Use one of: {', '.join(_ALLOWED_ROOTS)}"
    target = _resolve(subdir)
    if target is None or not target.exists():
        return f"not found: {subdir}"

    files = sorted(
        str(p.relative_to(config.REPO_ROOT))
        for p in target.rglob("*")
        if p.is_file()
        and not _DENY.search(str(p.relative_to(config.REPO_ROOT)))
        and p.suffix in (".py", ".sh", ".sql", ".tf", ".yml", ".yaml", ".toml")
    )
    if not files:
        return f"no source files under {subdir}"
    sizes = [f"{f}  ({(config.REPO_ROOT / f).stat().st_size // 1024}KB)" for f in files[:80]]
    return "\n".join(sizes)


@tool
def read_source_file(path: str, start_line: int = 1, end_line: int = 0) -> str:
    """Read a repo-relative source file, with line numbers.

    Reads at most 200 lines per call, so for a large file narrow the range using
    search_source first rather than paging through it. Line numbers in the output
    are real file line numbers -- quote them when you propose a patch.
    """
    target = _resolve(path)
    if target is None:
        return f"REJECTED: {path} is outside the repository or on the deny list."
    if not target.is_file():
        return f"not found: {path}"

    try:
        lines = target.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return f"could not read {path}: {exc}"

    start = max(1, start_line)
    end = len(lines) if end_line <= 0 else min(end_line, len(lines))
    end = min(end, start + _MAX_READ_LINES - 1)
    if start > len(lines):
        return f"{path} has only {len(lines)} lines"

    body = "\n".join(f"{i:>5}  {lines[i - 1]}" for i in range(start, end + 1))
    footer = (
        f"\n... showing lines {start}-{end} of {len(lines)}"
        if (start, end) != (1, len(lines))
        else ""
    )
    return f"--- {path} ---\n{body}{footer}"


@tool
def search_source(pattern: str, subdir: str = "src") -> str:
    """Search for a regex across source files and return matching lines with
    their paths and line numbers.

    This is how you find the line an error message came from. Search for a
    distinctive fragment of the traceback, not for a whole message -- log lines
    are usually formatted and will not match verbatim.
    """
    root = subdir.strip("/").split("/")[0]
    if root not in _ALLOWED_ROOTS:
        return f"REJECTED: '{root}' is not a searchable root."
    target = _resolve(subdir)
    if target is None or not target.exists():
        return f"not found: {subdir}"
    try:
        rx = re.compile(pattern)
    except re.error as exc:
        return f"REJECTED: bad regex: {exc}"

    hits: list[str] = []
    for p in sorted(target.rglob("*")):
        if not p.is_file() or p.suffix not in (".py", ".sh", ".sql", ".tf", ".yml", ".yaml"):
            continue
        rel = str(p.relative_to(config.REPO_ROOT))
        if _DENY.search(rel):
            continue
        try:
            for n, line in enumerate(
                p.read_text(encoding="utf-8", errors="replace").splitlines(), 1
            ):
                if rx.search(line):
                    hits.append(f"{rel}:{n}: {line.strip()[:160]}")
                    if len(hits) >= 40:
                        return (
                            "\n".join(hits) + "\n... (40 match limit reached, narrow the pattern)"
                        )
        except OSError:
            continue
    return "\n".join(hits) if hits else f"no matches for /{pattern}/ under {subdir}"


@tool
def recent_commits(count: int = 10) -> str:
    """Recent commit subjects with dates.

    Worth checking whenever something broke: a failure that started right after a
    commit that touched the same area is a far stronger lead than one that did
    not.
    """
    count = max(1, min(count, 30))
    try:
        out = subprocess.run(
            ["git", "log", f"-{count}", "--pretty=format:%h  %ad  %s", "--date=short"],
            cwd=config.REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"could not read git log: {exc}"
    return out.stdout.strip() or "no commits found"


REPO_TOOLS = [list_source_files, read_source_file, search_source, recent_commits]
