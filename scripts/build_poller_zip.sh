#!/usr/bin/env bash
# Build the poller Lambda as a plain zip. No Docker required.
#
# The poller needs protobuf and gtfs-realtime-bindings, which aren't in the
# Lambda runtime. A container image is one way to ship them; a zip built with
# manylinux wheels is another, and it costs ~1.7 MB instead of ~600 MB.
# pip's --platform flag cross-builds for Lambda's amd64 runtime, so this works
# identically on Apple Silicon and Intel.
set -euo pipefail

SRC="src/ingest/poller"
BUILD="build/poller"
OUT="build/poller.zip"

rm -rf "$BUILD" "$OUT"
mkdir -p "$BUILD"

pip install --quiet --target "$BUILD" \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --upgrade \
  -r "$SRC/requirements.txt"

cp "$SRC/handler.py" "$BUILD/"

# Strip test suites and metadata that Lambda never reads.
find "$BUILD" -type d -name "__pycache__"  -prune -exec rm -rf {} + 2>/dev/null || true
find "$BUILD" -type d -name "tests"        -prune -exec rm -rf {} + 2>/dev/null || true
find "$BUILD" -type d -name "*.dist-info"  -prune -exec rm -rf {} + 2>/dev/null || true

( cd "$BUILD" && zip -qr "../../$OUT" . )

SIZE="$(du -h "$OUT" | cut -f1)"
echo "built $OUT (${SIZE})"
echo "Lambda direct-upload limit is 50 MB zipped / 250 MB unzipped."
