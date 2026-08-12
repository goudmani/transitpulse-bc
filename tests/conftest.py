"""Make src/ importable in tests without installing the project."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for path in (ROOT / "src", ROOT / "src" / "ingest"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
