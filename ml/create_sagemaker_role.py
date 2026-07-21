"""
One-time (free) setup -- create the SageMaker execution role.

SageMaker runs training jobs under a ROLE it assumes (not your IAM user).
This creates a role that SageMaker can assume, with:
  * AmazonSageMakerFullAccess (managed), and
  * read/write to the pipeline bucket + the SageMaker default bucket.

Run:  python -m ml.create_sagemaker_role
Then copy the printed ARN into settings.SAGEMAKER_ROLE_ARN (or export
SAGEMAKER_ROLE_ARN) before running ml.train_xgboost.
"""

import json

from config import settings
from config.aws_session import get_client

ROLE_NAME = "cms-sagemaker-exec"
_SAGEMAKER_FULL_ACCESS = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"


def main():
    iam = get_client("iam")

    # ---- trust policy: SageMaker may assume this role ----
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sagemaker.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    try:
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Execution role for CMS pipeline SageMaker training jobs",
        )
        print(f"Created role {ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        role = {"Role": iam.get_role(RoleName=ROLE_NAME)["Role"]}
        print(f"Reusing existing role {ROLE_NAME}")

    # ---- managed SageMaker access ----
    iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=_SAGEMAKER_FULL_ACCESS)

    # ---- S3 access to our buckets (SageMakerFullAccess only covers
    #      buckets with 'sagemaker' in the name, so add ours explicitly) ----
    sm_default_bucket = f"sagemaker-{settings.AWS_REGION}-{settings.ACCOUNT_ID}"
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{settings.S3_BUCKET}",
                f"arn:aws:s3:::{settings.S3_BUCKET}/*",
                f"arn:aws:s3:::{sm_default_bucket}",
                f"arn:aws:s3:::{sm_default_bucket}/*",
            ],
        }],
    }
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="cms-bucket-access",
        PolicyDocument=json.dumps(s3_policy),
    )

    role_arn = role["Role"]["Arn"]
    print("\nRole ready.")
    print(f"SAGEMAKER_ROLE_ARN = {role_arn}")
    print("\nSet it before training, e.g.:")
    print(f'  set SAGEMAKER_ROLE_ARN={role_arn}')
    print("or paste it into config/settings.py -> SAGEMAKER_ROLE_ARN")


if __name__ == "__main__":
    main()
