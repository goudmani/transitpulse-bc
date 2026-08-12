#!/usr/bin/env bash
# Build the poller container and push it to ECR.
# --platform linux/amd64 is mandatory on Apple Silicon; without it the Lambda
# fails at runtime with an exec format error.
set -euo pipefail

REGION="${REGION:-ca-central-1}"
TAG="${TAG:-v1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
REPO="transitpulse/poller"
URI="${ACCT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:${TAG}"

aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "${REPO}" --region "${REGION}" \
       --image-scanning-configuration scanOnPush=true

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCT}.dkr.ecr.${REGION}.amazonaws.com"

docker build --platform linux/amd64 -t "${URI}" src/ingest/poller
docker push "${URI}"

echo "pushed ${URI}"
