import glob
import sys
from pathlib import Path

import hcl2

# Resolved from this file's own location, so the check works from any cwd.
REPO = Path(__file__).resolve().parents[1]

fails = []
files = sorted(glob.glob(str(REPO / "infra" / "**" / "*.tf"), recursive=True))
for f in files:
    try:
        with open(f) as fh:
            hcl2.load(fh)
    except Exception as e:
        fails.append((f, str(e).split("\n")[0][:220]))

print(f"parsed {len(files)} .tf files")
if fails:
    print(f"\nFAILURES ({len(fails)}):")
    for f, e in fails:
        print(f"  {f}\n     {e}")
    sys.exit(1)
print("all HCL parsed cleanly")
