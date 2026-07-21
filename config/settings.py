"""
Central configuration for the CMS Claims Analytics Pipeline.

Every script imports its resource names from here instead of hardcoding
them, so there is a single place to change a bucket, table, or ARN.

Nothing secret lives in this file -- only resource *names*. Credentials
are resolved separately by config/aws_session.py from your local AWS
profile, and are never written to source control.
"""

# ============================================================
# ACCOUNT / REGION
# ============================================================
AWS_PROFILE = "joel-admin"     # the local AWS CLI profile to authenticate with
AWS_REGION = "us-east-2"
ACCOUNT_ID = "933022095648"

# ============================================================
# S3 -- one bucket, several prefixes (folders)
# ============================================================
S3_BUCKET = "cms-medical-pipeline-jtz"

RAW_PREFIX = "raw/"                 # source CSV lands here
PROCESSED_PREFIX = "processed/"     # cleaned Parquet, partitioned by state
DQ_RESULTS_PREFIX = "dq-results/"   # Glue Data Quality rule outcomes (JSON)
ATHENA_RESULTS_PREFIX = "athena-results/"   # Athena query output
ML_PREFIX = "ml/"                   # ML train/validation input + model artifacts

# Convenience full-URI helpers
def s3_uri(prefix: str) -> str:
    """Return the full s3:// URI for a bucket prefix."""
    return f"s3://{S3_BUCKET}/{prefix}"

# ============================================================
# GLUE (Data Catalog + ETL job)
# ============================================================
GLUE_DATABASE = "cms_medical_db"
RAW_TABLE = "raw"
PROCESSED_TABLE = "processed"
GLUE_JOB = "cms-etl-transform"

# ============================================================
# ORCHESTRATION (Step Functions + SNS + EventBridge)
# ============================================================
STATE_MACHINE_NAME = "cms-pipeline-orchestrator"
STATE_MACHINE_ARN = (
    f"arn:aws:states:{AWS_REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
)

SNS_TOPIC_NAME = "cms-pipeline-alerts"
SNS_TOPIC_ARN = f"arn:aws:sns:{AWS_REGION}:{ACCOUNT_ID}:{SNS_TOPIC_NAME}"

# The S3-triggered Lambda that validates uploads and starts the state machine.
LAMBDA_FUNCTION = "cms-file-validator"

# ============================================================
# ATHENA
# ============================================================
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_LOCATION = s3_uri(ATHENA_RESULTS_PREFIX)

# ============================================================
# MACHINE LEARNING (SageMaker)
# ============================================================
# Ephemeral, cost-conscious defaults: a small instance and managed spot
# training so nothing is left running after a training job finishes.
SAGEMAKER_INSTANCE_TYPE = "ml.m5.large"
SAGEMAKER_USE_SPOT = True
SAGEMAKER_MAX_RUNTIME_SEC = 1800     # hard 30-min ceiling per training job
ML_TRAIN_PREFIX = f"{ML_PREFIX}input/"
ML_MODEL_PREFIX = f"{ML_PREFIX}model/"

# SageMaker needs an EXECUTION ROLE (a role it assumes, with S3 + SageMaker
# access) -- an IAM *user* can't be used directly. Set this to your role ARN
# (or export SAGEMAKER_ROLE_ARN). Leave "" until you've created one; the
# training launcher will tell you how if it's missing.
SAGEMAKER_ROLE_ARN = ""
