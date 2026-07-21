"""
ML step 2 -- launch a SageMaker XGBoost training job via boto3.

Uses SageMaker's BUILT-IN XGBoost algorithm through the plain boto3
SageMaker client -- deliberately NO sagemaker SDK (its v3 rewrite broke the
classic API, and boto3 is stable across versions).

!!! THIS COSTS MONEY !!! (a small, spot, time-capped training job -- cents)
Kept cheap by: ml.m5.large + managed spot + a 30-min max_run + no endpoint.

Prerequisite: a SageMaker execution role -- run ml.create_sagemaker_role,
then set settings.SAGEMAKER_ROLE_ARN or export SAGEMAKER_ROLE_ARN.

Run:  python -m ml.train_xgboost
"""

import os
import time

from config import settings
from config.aws_session import get_client

_TERMINAL = {"Completed", "Failed", "Stopped"}
_POLL_SECONDS = 20


# ============================================================
# Resolve the SageMaker execution role
# ============================================================
def _resolve_role() -> str:
    role = os.environ.get("SAGEMAKER_ROLE_ARN") or settings.SAGEMAKER_ROLE_ARN
    if not role:
        raise SystemExit(
            "No SageMaker execution role set. Run:\n"
            "  python -m ml.create_sagemaker_role\n"
            "then set settings.SAGEMAKER_ROLE_ARN or export SAGEMAKER_ROLE_ARN."
        )
    return role


# ============================================================
# Build the training-job request
# ============================================================
def _build_request(job_name: str, role: str) -> dict:
    base = settings.s3_uri(settings.ML_TRAIN_PREFIX)

    def _channel(name: str, filename: str) -> dict:
        return {
            "ChannelName": name,
            "ContentType": "text/csv",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"{base}{filename}",
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
        }

    request = {
        "TrainingJobName": job_name,
        "AlgorithmSpecification": {
            "TrainingImage": settings.XGBOOST_IMAGE_URI,
            "TrainingInputMode": "File",
        },
        "RoleArn": role,
        "InputDataConfig": [
            _channel("train", "train.csv"),
            _channel("validation", "validation.csv"),
        ],
        "OutputDataConfig": {"S3OutputPath": settings.s3_uri(settings.ML_MODEL_PREFIX)},
        "ResourceConfig": {
            "InstanceType": settings.SAGEMAKER_INSTANCE_TYPE,
            "InstanceCount": 1,
            "VolumeSizeInGB": 10,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": settings.SAGEMAKER_MAX_RUNTIME_SEC},
        "HyperParameters": {
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "num_round": "300",
            "max_depth": "6",
            "eta": "0.1",
            "subsample": "0.8",
            "colsample_bytree": "0.8",
            # positive (outlier) class is ~1% -> weight it ~ neg/pos = 99
            "scale_pos_weight": "99",
        },
    }

    if settings.SAGEMAKER_USE_SPOT:
        request["EnableManagedSpotTraining"] = True
        request["StoppingCondition"]["MaxWaitTimeInSeconds"] = (
            settings.SAGEMAKER_MAX_RUNTIME_SEC + 600
        )

    return request


# ============================================================
# MAIN -- create the job and poll to completion
# ============================================================
def main():
    sm = get_client("sagemaker")
    job_name = f"cms-xgb-{int(time.time())}"

    print(f"Starting SageMaker training job {job_name} (billable) ...")
    sm.create_training_job(**_build_request(job_name, _resolve_role()))

    while True:
        desc = sm.describe_training_job(TrainingJobName=job_name)
        status = desc["TrainingJobStatus"]
        if status in _TERMINAL:
            break
        print(f"  {status} / {desc.get('SecondaryStatus', '')} ...")
        time.sleep(_POLL_SECONDS)

    print(f"\nTraining job {status}.")
    if status != "Completed":
        raise SystemExit(desc.get("FailureReason", "See SageMaker console for details."))

    print(f"Model artifact: {desc['ModelArtifacts']['S3ModelArtifacts']}")
    for metric in desc.get("FinalMetricDataList", []):
        print(f"  {metric['MetricName']}: {metric['Value']:.4f}")
    print("Next: python -m ml.score   (scores locally, no endpoint cost)")


if __name__ == "__main__":
    main()
