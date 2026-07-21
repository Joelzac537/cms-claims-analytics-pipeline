"""
ML step 2 -- launch the SageMaker XGBoost training job.

!!! THIS COSTS MONEY !!!
It starts a managed training instance on AWS. Costs are kept small by:
  * a small instance (ml.m5.large),
  * managed SPOT instances (~70% cheaper),
  * a hard 30-minute max_run ceiling,
  * NO persistent inference endpoint (we score locally in ml/score.py).
Still, only run this from your terminal when you intend to spend:
    python -m ml.train_xgboost

Prerequisite: a SageMaker execution role. Set settings.SAGEMAKER_ROLE_ARN
(or export SAGEMAKER_ROLE_ARN). This is a role SageMaker assumes -- an IAM
user cannot be used directly.
"""

import os
from pathlib import Path

import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.xgboost import XGBoost

from config import settings
from config.aws_session import get_session

_SRC_DIR = Path(__file__).parent / "sagemaker_src"


# ============================================================
# Resolve the SageMaker execution role
# ============================================================
def _resolve_role() -> str:
    role = os.environ.get("SAGEMAKER_ROLE_ARN") or settings.SAGEMAKER_ROLE_ARN
    if not role:
        raise SystemExit(
            "No SageMaker execution role set.\n"
            "Create a role trusting sagemaker.amazonaws.com with "
            "AmazonSageMakerFullAccess + S3 access to the pipeline bucket, "
            "then set settings.SAGEMAKER_ROLE_ARN or export SAGEMAKER_ROLE_ARN."
        )
    return role


# ============================================================
# Build the estimator
# ============================================================
def _build_estimator() -> XGBoost:
    sm_session = sagemaker.Session(boto_session=get_session())
    max_wait = settings.SAGEMAKER_MAX_RUNTIME_SEC + 600  # spot queue slack

    return XGBoost(
        entry_point="train.py",
        source_dir=str(_SRC_DIR),
        framework_version="1.7-1",
        role=_resolve_role(),
        instance_type=settings.SAGEMAKER_INSTANCE_TYPE,
        instance_count=1,
        output_path=settings.s3_uri(settings.ML_MODEL_PREFIX),
        sagemaker_session=sm_session,
        use_spot_instances=settings.SAGEMAKER_USE_SPOT,
        max_run=settings.SAGEMAKER_MAX_RUNTIME_SEC,
        max_wait=max_wait if settings.SAGEMAKER_USE_SPOT else None,
        hyperparameters={
            "n-estimators": 300,
            "max-depth": 6,
            "learning-rate": 0.1,
        },
    )


# ============================================================
# MAIN -- point at the S3 channels and fit
# ============================================================
def main():
    base = settings.s3_uri(settings.ML_TRAIN_PREFIX)
    channels = {
        "train": TrainingInput(f"{base}train.csv", content_type="text/csv"),
        "validation": TrainingInput(f"{base}validation.csv", content_type="text/csv"),
    }

    estimator = _build_estimator()
    print("Starting SageMaker training job (billable) ...")
    estimator.fit(channels)

    print("\nTraining complete.")
    print(f"Model artifact: {estimator.model_data}")
    print("Next: python -m ml.score   (scores locally, no endpoint cost)")


if __name__ == "__main__":
    main()
