"""Define and upsert the SageMaker Pipeline: train -> evaluate -> gate -> register.

Run this locally (or from CI) to create or update the pipeline definition:
    python src/ml/pipeline.py --role-arn ... --gold-bucket ... --artifacts-bucket ...
Then start executions with the AWS CLI or the weekly EventBridge rule.
"""

from __future__ import annotations

import argparse

import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.fail_step import FailStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.parameters import ParameterFloat, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.xgboost.estimator import XGBoost

FRAMEWORK_VERSION = "1.7-1"


def build(args: argparse.Namespace) -> Pipeline:
    session = sagemaker.session.Session()
    region = session.boto_region_name

    train_uri = ParameterString(
        "TrainUri", default_value=f"s3://{args.gold_bucket}/features/split/train/"
    )
    val_uri = ParameterString(
        "ValidationUri", default_value=f"s3://{args.gold_bucket}/features/split/val/"
    )
    test_uri = ParameterString(
        "TestUri", default_value=f"s3://{args.gold_bucket}/features/split/test/"
    )
    # Must beat "the bus stays as late as it currently is" by this margin.
    max_ratio = ParameterFloat("MaxMaeRatioVsPersistence", default_value=0.92)

    estimator = XGBoost(
        entry_point="train.py",
        source_dir=args.source_dir,
        framework_version=FRAMEWORK_VERSION,
        py_version="py3",
        role=args.role_arn,
        instance_type=args.train_instance_type,
        instance_count=1,
        output_path=f"s3://{args.artifacts_bucket}/models/",
        base_job_name=f"{args.project}-train",
        use_spot_instances=True,
        max_run=3600,
        max_wait=7200,
        hyperparameters={"num-round": 800, "max-depth": 8, "eta": 0.08},
        metric_definitions=[
            {"Name": "validation:mae", "Regex": r"validation:mae=([0-9\.]+)"},
            {"Name": "validation:rmse", "Regex": r"validation:rmse=([0-9\.]+)"},
        ],
        sagemaker_session=session,
    )

    train_step = TrainingStep(
        name="TrainDelayModel",
        estimator=estimator,
        inputs={
            "train": TrainingInput(s3_data=train_uri, content_type="application/x-parquet"),
            "validation": TrainingInput(s3_data=val_uri, content_type="application/x-parquet"),
        },
    )

    image_uri = sagemaker.image_uris.retrieve(
        framework="xgboost",
        region=region,
        version=FRAMEWORK_VERSION,
        py_version="py3",
        instance_type=args.eval_instance_type,
    )

    processor = ScriptProcessor(
        image_uri=image_uri,
        command=["python3"],
        role=args.role_arn,
        instance_type=args.eval_instance_type,
        instance_count=1,
        base_job_name=f"{args.project}-eval",
        sagemaker_session=session,
    )

    report = PropertyFile(name="EvaluationReport", output_name="evaluation", path="evaluation.json")

    eval_step = ProcessingStep(
        name="EvaluateAgainstBaselines",
        processor=processor,
        code=f"{args.source_dir}/evaluate.py",
        inputs=[
            ProcessingInput(
                source=train_step.properties.ModelArtifacts.S3ModelArtifacts,
                destination="/opt/ml/processing/model",
            ),
            ProcessingInput(source=test_uri, destination="/opt/ml/processing/test"),
        ],
        outputs=[
            ProcessingOutput(
                output_name="evaluation",
                source="/opt/ml/processing/evaluation",
                destination=f"s3://{args.artifacts_bucket}/evaluation/",
            )
        ],
        property_files=[report],
    )

    register_step = train_step.estimator.register(
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name=args.model_package_group,
        approval_status="PendingManualApproval",
    )

    gate = ConditionLessThanOrEqualTo(
        left=JsonGet(
            step_name=eval_step.name,
            property_file=report,
            json_path="metrics.mae_ratio_vs_persistence",
        ),
        right=max_ratio,
    )

    condition_step = ConditionStep(
        name="GateOnBaselineImprovement",
        conditions=[gate],
        if_steps=[register_step],
        else_steps=[
            FailStep(
                name="RejectModel",
                error_message="Model did not beat the persistence baseline by the required margin.",
            )
        ],
    )

    return Pipeline(
        name=f"{args.project}-training",
        parameters=[train_uri, val_uri, test_uri, max_ratio],
        steps=[train_step, eval_step, condition_step],
        sagemaker_session=session,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="transitpulse")
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--gold-bucket", required=True)
    parser.add_argument("--artifacts-bucket", required=True)
    parser.add_argument("--model-package-group", default="transitpulse")
    parser.add_argument("--source-dir", default="src/ml")
    parser.add_argument("--train-instance-type", default="ml.m5.xlarge")
    parser.add_argument("--eval-instance-type", default="ml.m5.large")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = build(args)
    pipeline.upsert(role_arn=args.role_arn)
    print(f"upserted pipeline: {pipeline.name}")


if __name__ == "__main__":
    main()
