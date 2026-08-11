#!/usr/bin/env bash
# One-time: create the Terraform state bucket and lock table.
set -euo pipefail

REGION="${REGION:-ca-central-1}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="tfstate-transitpulse-${ACCT}"

echo "account=${ACCT} region=${REGION} bucket=${BUCKET}"

if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "state bucket already exists"
else
  if [ "${REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}"
  fi
fi

aws s3api put-bucket-versioning --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled

aws s3api put-public-access-block --bucket "${BUCKET}" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-encryption --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

if aws dynamodb describe-table --table-name tfstate-lock --region "${REGION}" >/dev/null 2>&1; then
  echo "lock table already exists"
else
  aws dynamodb create-table --table-name tfstate-lock \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST --region "${REGION}"
  aws dynamodb wait table-exists --table-name tfstate-lock --region "${REGION}"
fi

echo "bootstrap complete. next: make init"
