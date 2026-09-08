"""Define and upsert the SageMaker Pipeline: train -> evaluate -> gate -> register.

Run this locally (or from CI) to create or update the pipeline definition:
    python src/ml/pipeline.py --role-arn ... --gold-bucket ... --artifacts-bucket ...
Then start executions with the AWS CLI or the weekly EventBridge rule.
"""

from __future__ import annotations

import argparse

import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.model import Model
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.fail_step import FailStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.model_step import ModelStep
from sagemaker.workflow.parameters import ParameterFloat, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.xgboost.estimator import XGBoost

FRAMEWORK_VERSION = "1.7-1"


def build(args: argparse.Namespace) -> Pipeline:
    # PipelineSession, not Session. Under a plain Session, estimator.fit() /
    # processor.run() / model.register() CALL the AWS API immediately; under a
    # PipelineSession they capture the arguments and return them for a step to
    # hold, resolving pipeline variables at execution time instead. With a plain
    # Session, register() tried to create a real model package whose ModelDataUrl
    # was an unresolved step property, and botocore rejected it.
    session = PipelineSession()
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
    # Must beat the STRONGEST simple baseline by this margin -- whichever of
    # schedule / persistence / historical-median wins on the test set. Measured on
    # the 2026-08-11..09-06 test split: historical 127.3s, persistence 130.2s,
    # schedule 152.1s. Gating on persistence alone would have admitted a model at
    # 125s that a route/stop/hour lookup table already beats.
    max_ratio = ParameterFloat("MaxMaeRatioVsBestBaseline", default_value=0.92)

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
        use_spot_instances=not args.no_spot,
        # 2h, not 1h. max_run does not cap the bill, it KILLS the job -- a timeout
        # costs exactly as much as a successful run and produces no model. 800
        # rounds of reg:absoluteerror over 7.2M rows on 4 vCPUs may or may not fit
        # in an hour, and early stopping should end it long before either limit.
        max_run=7200,
        # max_wait covers queue time PLUS run time and must exceed max_run.
        # Only meaningful for spot; SageMaker rejects it on on-demand.
        max_wait=None if args.no_spot else 10800,
        hyperparameters={"num-round": 800, "max-depth": 8, "eta": 0.08},
        # No metric_definitions. sagemaker-xgboost is a FIRST-PARTY image, and
        # CreateTrainingJob rejects AlgorithmSpecification.MetricDefinitions for
        # 1P algorithms outright -- "You can't override the metric definitions for
        # Amazon SageMaker algorithms" -- even in script mode, because script mode
        # reuses the same image as the built-in algorithm. The container already
        # publishes validation:mae and validation:rmse from the standard XGBoost
        # eval output, and evaluate.py is the authoritative scorer regardless.
        # The `validation:mae=` line train.py prints stays: it is human-readable
        # in CloudWatch and is what a future HyperparameterTuner would scrape.
        sagemaker_session=session,
    )

    train_step = TrainingStep(
        name="TrainDelayModel",
        step_args=estimator.fit(
            inputs={
                "train": TrainingInput(s3_data=train_uri, content_type="application/x-parquet"),
                "validation": TrainingInput(
                    s3_data=val_uri, content_type="application/x-parquet"
                ),
            }
        ),
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
        step_args=processor.run(
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
        ),
        property_files=[report],
    )

    # Estimator.register() reads estimator.model_data, which only exists after a
    # real fit() -- inside a pipeline the artifact is a step property resolved at
    # runtime, so that call fails at definition time with model_data = None.
    # Build a Model that points at the training step's output instead.
    model = Model(
        image_uri=image_uri,
        model_data=train_step.properties.ModelArtifacts.S3ModelArtifacts,
        role=args.role_arn,
        sagemaker_session=session,
    )

    # Attaches the evaluation report to the registry entry, so the approver sees
    # the test-set MAE and the baseline it beat rather than an opaque model ARN.
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=f"s3://{args.artifacts_bucket}/evaluation/evaluation.json",
            content_type="application/json",
        )
    )

    register_step = ModelStep(
        name="RegisterModel",
        step_args=model.register(
            content_types=["text/csv"],
            response_types=["text/csv"],
            inference_instances=["ml.m5.large"],
            transform_instances=["ml.m5.large"],
            model_package_group_name=args.model_package_group,
            approval_status="PendingManualApproval",
            model_metrics=model_metrics,
        ),
    )

    gate = ConditionLessThanOrEqualTo(
        left=JsonGet(
            step_name=eval_step.name,
            property_file=report,
            json_path="metrics.mae_ratio_vs_best_baseline",
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
                error_message="Model did not beat the strongest simple baseline by the required margin.",
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
    # ml.t3.xlarge, not ml.m5.large: a fresh account's quota for every m5
    # processing instance is 0, while ml.t3.xlarge is 2. It is also the larger
    # machine (16 GB vs 8 GB), which matters because evaluate.py loads all 4.16M
    # test rows into pandas before building the DMatrix.
    parser.add_argument("--eval-instance-type", default="ml.t3.xlarge")
    # Spot and on-demand are SEPARATE quotas. If only one increase is approved,
    # flip this rather than waiting on the other.
    parser.add_argument("--no-spot", action="store_true", help="disable managed spot training")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = build(args)
    pipeline.upsert(role_arn=args.role_arn)
    print(f"upserted pipeline: {pipeline.name}")


if __name__ == "__main__":
    main()
