#!/usr/bin/env bash
# Preflight: verify this machine can build the project before you spend money.
# Safe to run repeatedly. Makes no changes and creates no AWS resources.

set -uo pipefail

PASS=0
WARN=0
FAIL=0

ok()   { printf "  \033[32mPASS\033[0m  %s\n" "$1"; PASS=$((PASS+1)); }
warn() { printf "  \033[33mWARN\033[0m  %s\n" "$1"; WARN=$((WARN+1)); }
bad()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAIL=$((FAIL+1)); }

ver_ge() {
  # ver_ge 1.9.5 1.6.0  -> true when $1 >= $2
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" = "$2" ]
}

echo "=============================================="
echo " TransitPulse preflight"
echo "=============================================="
echo
echo "--- machine ---"

OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
  Darwin)
    MACVER="$(sw_vers -productVersion 2>/dev/null || echo unknown)"
    ok "macOS ${MACVER} (${ARCH})"
    if [ "$ARCH" = "arm64" ]; then
      warn "Apple Silicon: Docker images MUST build with --platform linux/amd64"
      echo "        scripts/build_push_poller.sh already does this for you."
    fi
    ;;
  Linux) ok "Linux (${ARCH})" ;;
  *)     warn "Unrecognised OS: ${OS}" ;;
esac

FREE_GB="$(df -Pg . 2>/dev/null | awk 'NR==2 {print $4}')"
if [ -n "${FREE_GB:-}" ]; then
  if [ "${FREE_GB}" -ge 5 ]; then
    ok "disk free: ${FREE_GB} GB (zip build path needs ~3 GB total)"
  elif [ "${FREE_GB}" -ge 3 ]; then
    warn "disk free: ${FREE_GB} GB -- tight. Skip requirements-ml.txt and run 'make clean' often."
  else
    bad "disk free: ${FREE_GB} GB -- need at least 3 GB"
  fi
fi

echo
echo "--- required tools ---"

check_tool() {
  name="$1"; min="$2"; cmd="$3"
  if ! command -v "$name" >/dev/null 2>&1; then
    bad "$name not installed"
    return
  fi
  got="$(eval "$cmd" 2>/dev/null | head -1)"
  if [ -z "$got" ]; then
    warn "$name installed, version unreadable"
  elif ver_ge "$got" "$min"; then
    ok "$name $got (need >= $min)"
  else
    bad "$name $got is older than $min"
  fi
}

check_tool aws       2.13.0 "aws --version 2>&1 | sed -E 's|aws-cli/([0-9.]+).*|\1|'"
check_tool terraform 1.6.0  "terraform version | head -1 | sed -E 's/Terraform v([0-9.]+).*/\1/'"
check_tool git       2.30.0 "git --version | sed -E 's/git version ([0-9.]+).*/\1/'"
check_tool python3   3.11.0 "python3 --version | sed -E 's/Python ([0-9.]+).*/\1/'"
check_tool jq        1.6    "jq --version | sed -E 's/jq-([0-9.]+).*/\1/'"

if command -v gh >/dev/null 2>&1; then
  ok "gh $(gh --version | head -1 | sed -E 's/gh version ([0-9.]+).*/\1/')"
else
  warn "gh not installed (only needed to create the GitHub repo from the terminal)"
fi

echo
echo "--- docker (OPTIONAL) ---"
# The default build path is a ~2 MB zip. Docker is only needed if you choose
# poller_package_type = "Image".
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ok "docker running (optional -- only needed for the Image build path)"
else
  ok "docker not running -- fine, the default Zip path does not need it"
fi

echo
echo "--- build artifacts ---"
if [ -f build/poller.zip ]; then
  ZSIZE="$(du -h build/poller.zip | cut -f1)"
  ok "build/poller.zip present (${ZSIZE})"
else
  warn "build/poller.zip missing -- run: make package"
fi

echo
echo "--- aws credentials ---"
if IDENT="$(aws sts get-caller-identity --output json 2>/dev/null)"; then
  ACCT="$(echo "$IDENT" | jq -r .Account)"
  ARN="$(echo "$IDENT" | jq -r .Arn)"
  ok "authenticated as ${ARN}"
  ok "account ${ACCT}"

  case "$ARN" in
    *voclabs*|*LabRole*)
      bad "this looks like an AWS Academy Learner Lab"
      echo "        Learner Labs shut down between sessions and cannot run a"
      echo "        minute-by-minute poller. Use a personal AWS account."
      ;;
  esac

  REGION="$(aws configure get region 2>/dev/null || echo "${AWS_REGION:-}")"
  if [ -n "$REGION" ]; then
    ok "default region: ${REGION}"
    [ "$REGION" = "ca-central-1" ] || warn "guide assumes ca-central-1; update envs/dev.tfvars if intentional"
  else
    bad "no default region set -- run: aws configure set region ca-central-1"
  fi
else
  bad "aws sts get-caller-identity failed -- credentials not configured"
fi

echo
echo "--- service reachability (read-only calls) ---"
probe() {
  label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label reachable"; else warn "$label call failed (permissions or region)"; fi
}
if aws sts get-caller-identity >/dev/null 2>&1; then
  probe "s3"          aws s3api list-buckets
  probe "kinesis"     aws kinesis list-streams
  probe "glue"        aws glue get-databases
  probe "sagemaker"   aws sagemaker list-training-jobs --max-results 1
  probe "stepfunctions" aws stepfunctions list-state-machines --max-results 1
  probe "ecr"         aws ecr describe-repositories --max-results 1
fi

echo
echo "--- python environment ---"
if python3 -c "import sys; sys.exit(0 if sys.prefix != sys.base_prefix else 1)" 2>/dev/null; then
  ok "running inside a virtualenv"
else
  warn "not in a virtualenv -- run: python3 -m venv .venv && source .venv/bin/activate"
fi

for pkg in ruff pytest boto3 requests; do
  if python3 -c "import ${pkg}" 2>/dev/null || command -v "$pkg" >/dev/null 2>&1; then
    ok "python: ${pkg}"
  else
    warn "python: ${pkg} missing -- pip install -r requirements-dev.txt"
  fi
done

echo
echo "--- project config ---"
if [ -f infra/envs/dev.tfvars ]; then
  if grep -q "CHANGE_ME" infra/envs/dev.tfvars; then
    bad "infra/envs/dev.tfvars still contains CHANGE_ME -- set alert_email"
  else
    ok "dev.tfvars customised"
  fi
else
  warn "infra/envs/dev.tfvars not found -- are you in the repo root?"
fi

if [ -n "${TL_KEY:-}" ]; then
  ok "TL_KEY is set in the environment"
else
  warn "TL_KEY not set -- export TL_KEY=... before testing the feed"
fi

echo
echo "=============================================="
printf " %d passed, %d warnings, %d failures\n" "$PASS" "$WARN" "$FAIL"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
  echo
  echo "Fix the failures above before running 'make plan'."
  exit 1
fi
echo
echo "Ready. Next: make lint && make test"
