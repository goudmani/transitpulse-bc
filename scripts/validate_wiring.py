"""Cross-module consistency checks that HCL syntax parsing cannot catch."""

import glob
import os
import re
import sys
from pathlib import Path

import hcl2

# Resolved from this file's own location, so the checker gives the same answer
# whether you run it from the repo root, from scripts/, or from CI.
REPO = Path(__file__).resolve().parents[1]
ROOT = str(REPO / "infra")
SRC = str(REPO)
errors, notes = [], []


def load_dir(d):
    merged = {}
    for f in glob.glob(os.path.join(d, "*.tf")):
        with open(f) as fh:
            doc = hcl2.load(fh)
        for k, v in doc.items():
            merged.setdefault(k, []).extend(v)
    return merged


def unq(x):
    return x.strip('"') if isinstance(x, str) else x


def names(blocks, kind):
    out = set()
    for b in blocks.get(kind, []):
        out |= {unq(k) for k in b}
    return out


def raw_text(d):
    parts = []
    for f in glob.glob(os.path.join(d, "*.tf")):
        with open(f) as fh:
            parts.append(fh.read())
    return "\n".join(parts)


# ---- module inventory ----
modules = {}
for path in sorted(glob.glob(os.path.join(ROOT, "modules", "*"))):
    if not os.path.isdir(path):
        continue
    name = os.path.basename(path)
    blocks = load_dir(path)
    modules[name] = {
        "path": path,
        "vars": names(blocks, "variable"),
        "outputs": names(blocks, "output"),
        "text": raw_text(path),
        "blocks": blocks,
    }

root_blocks = load_dir(ROOT)
root_text = raw_text(ROOT)
root_vars = names(root_blocks, "variable")

# ---- 1. module call args vs declared variables ----
declared_calls = {}
for m in root_blocks.get("module", []):
    for call_name, body in m.items():
        declared_calls[unq(call_name)] = body

for call_name, body in declared_calls.items():
    if call_name not in modules:
        errors.append(f"root calls module '{call_name}' but no modules/{call_name} dir")
        continue
    mod = modules[call_name]
    passed = {
        unq(k)
        for k in body
        if unq(k)
        not in ("source", "providers", "version", "count", "for_each", "depends_on", "__is_block__")
    }
    unknown = passed - mod["vars"]
    required = set()
    for b in mod["blocks"].get("variable", []):
        for vname, vbody in b.items():
            if "default" not in vbody:
                required.add(unq(vname))
    missing = required - passed
    for u in sorted(unknown):
        errors.append(f"module.{call_name}: passes '{u}' which is not a declared variable")
    for mi in sorted(missing):
        errors.append(f"module.{call_name}: required variable '{mi}' not supplied")

# ---- 2. module.X.Y references resolve to real outputs ----
for mod_name, attr in set(re.findall(r"module\.([a-z_]+)\.([a-z_]+)", root_text)):
    if mod_name not in modules:
        errors.append(f"reference module.{mod_name} has no matching module directory")
    elif attr not in modules[mod_name]["outputs"]:
        errors.append(f"module.{mod_name}.{attr} referenced but '{attr}' is not an output")

# ---- 3. every var.X inside a module is declared there ----
for name, mod in modules.items():
    for used in set(re.findall(r"\bvar\.([a-z_]+)", mod["text"])):
        if used not in mod["vars"]:
            errors.append(f"modules/{name}: uses var.{used} which is not declared")

for used in set(re.findall(r"\bvar\.([a-z_]+)", root_text)):
    if used not in root_vars:
        errors.append(f"root: uses var.{used} which is not declared")

# ---- 4. declared but unused variables (warning only) ----
for name, mod in modules.items():
    for v in sorted(mod["vars"]):
        if len(re.findall(rf"\bvar\.{v}\b", mod["text"])) == 0:
            notes.append(f"modules/{name}: variable '{v}' declared but never used")

# ---- 5. file paths referenced from Terraform actually exist ----
for match in set(
    re.findall(
        r'source_dir\s*=\s*"\$\{path\.module\}/([^"]+)"',
        root_text + "".join(m["text"] for m in modules.values()),
    )
):
    for mod in modules.values():
        candidate = os.path.normpath(os.path.join(mod["path"], match))
        if os.path.isdir(candidate):
            break
    else:
        errors.append(f"archive_file source_dir does not resolve: {match}")

# ---- 6. glue scripts referenced exist ----
if not modules:
    errors.append(f"no module directories found under {ROOT}/modules -- check ROOT")
elif "etl" not in modules:
    errors.append(f"no etl module directory under {ROOT}/modules")
else:
    for script in re.findall(r'"(\w+\.py)"', modules["etl"]["text"]):
        if not os.path.exists(os.path.join(SRC, "src", "glue", script)):
            errors.append(f"etl module references src/glue/{script} which does not exist")

# ---- 7. tfvars supply every required root variable ----
required_root = set()
for b in root_blocks.get("variable", []):
    for vname, vbody in b.items():
        if "default" not in vbody:
            required_root.add(unq(vname))
for tfvars in glob.glob(os.path.join(ROOT, "envs", "*.tfvars")):
    with open(tfvars) as fh:
        supplied = set(re.findall(r"^\s*([a-z_]+)\s*=", fh.read(), re.M))
    missing = required_root - supplied
    for mi in sorted(missing):
        errors.append(f"{tfvars}: missing required variable '{mi}'")

# ---- 8. Lambda handler entrypoints match Terraform handler strings ----
for handler_ref in re.findall(
    r'handler\s*=\s*"([\w.]+)"', "".join(m["text"] for m in modules.values())
):
    mod_name, fn = handler_ref.rsplit(".", 1)
    if fn != "lambda_handler":
        errors.append(f"unexpected Lambda handler entrypoint: {handler_ref}")

print(f"modules found: {sorted(modules)}")
print(f"module calls in root: {sorted(declared_calls)}")
print()
if errors:
    print(f"ERRORS ({len(errors)}):")
    for e in errors:
        print("  -", e)
else:
    print("wiring checks: PASS")
if notes:
    print(f"\nnotes ({len(notes)}):")
    for n in notes:
        print("  -", n)
sys.exit(1 if errors else 0)
