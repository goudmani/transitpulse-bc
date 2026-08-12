"""Deploy the latest approved model package to a serverless endpoint.

Serverless inference scales to zero, which is the difference between a few
dollars a month and ninety for a portfolio project nobody is calling.
"""

from __future__ import annotations

import argparse

import boto3
import sagemaker
from sagemaker import ModelPackage
from sagemaker.serverless import ServerlessInferenceConfig


def latest_approved(group: str, region: str) -> str:
    client = boto3.client("sagemaker", region_name=region)
    response = client.list_model_packages(
        ModelPackageGroupName=group,
        ModelApprovalStatus="Approved",
        SortBy="CreationTime",
        SortOrder="Descending",
        MaxResults=1,
    )
    packages = response.get("ModelPackageSummaryList", [])
    if not packages:
        raise RuntimeError(f"no approved model packages in group {group}")
    return packages[0]["ModelPackageArn"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--model-package-group", default="transitpulse")
    parser.add_argument("--endpoint-name", default="transitpulse-delay-predictor")
    parser.add_argument("--gold-bucket", required=True)
    parser.add_argument("--memory-mb", type=int, default=2048)
    parser.add_argument("--max-concurrency", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = sagemaker.session.Session()
    region = session.boto_region_name

    package_arn = latest_approved(args.model_package_group, region)
    print(f"deploying {package_arn}")

    model = ModelPackage(
        role=args.role_arn,
        model_package_arn=package_arn,
        sagemaker_session=session,
    )

    model.deploy(
        endpoint_name=args.endpoint_name,
        serverless_inference_config=ServerlessInferenceConfig(
            memory_size_in_mb=args.memory_mb,
            max_concurrency=args.max_concurrency,
        ),
    )
    print(f"endpoint live: {args.endpoint_name}")


if __name__ == "__main__":
    main()
