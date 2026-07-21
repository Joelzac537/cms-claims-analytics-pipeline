"""
Milestone 6 -- deploy the Lambda -> Step Functions orchestration.

Wires up:  S3 upload -> cms-file-validator (validate) -> state machine
                                                            (Glue + DQ -> SNS)

This script updates THREE things on AWS (all reversible):
    1. grants the Lambda's role permission to start the state machine,
    2. updates the Lambda's code (lambda_functions/cms_file_validator.py),
    3. updates the state-machine definition (removes the recursive
       quality-check step; now Glue -> SNS).

The existing S3 -> Lambda notification is left untouched (no bucket-config
overwrite), so nothing that already works is disturbed.

Run:  python -m orchestration.deploy_orchestration
"""

import io
import json
import zipfile
from pathlib import Path

from config import settings
from config.aws_session import get_client

_ROOT = Path(__file__).parents[1]
LAMBDA_SRC = _ROOT / "lambda_functions" / "cms_file_validator.py"
SFN_DEF = _ROOT / "step_functions" / "cms_pipeline_orchestrator.json"


# ============================================================
# STEP 1 -- let the Lambda's role start the state machine
# ============================================================
def grant_lambda_start_sfn() -> dict:
    """Attach states:StartExecution to the Lambda's execution role."""
    cfg = get_client("lambda").get_function(
        FunctionName=settings.LAMBDA_FUNCTION
    )["Configuration"]
    role_name = cfg["Role"].split("/")[-1]

    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": settings.STATE_MACHINE_ARN,
        }],
    }
    get_client("iam").put_role_policy(
        RoleName=role_name,
        PolicyName="start-cms-state-machine",
        PolicyDocument=json.dumps(policy),
    )
    print(f"Granted states:StartExecution to role {role_name}")
    return cfg


# ============================================================
# STEP 2 -- update the Lambda's code (zip in memory, upload)
# ============================================================
def update_lambda_code(cfg: dict):
    module = cfg["Handler"].split(".")[0]  # e.g. "cms_file_validator"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{module}.py", LAMBDA_SRC.read_text(encoding="utf-8"))
    buf.seek(0)

    get_client("lambda").update_function_code(
        FunctionName=settings.LAMBDA_FUNCTION,
        ZipFile=buf.read(),
    )
    print(f"Updated Lambda code for {settings.LAMBDA_FUNCTION}")


# ============================================================
# STEP 3 -- update the state-machine definition
# ============================================================
def update_state_machine():
    get_client("stepfunctions").update_state_machine(
        stateMachineArn=settings.STATE_MACHINE_ARN,
        definition=SFN_DEF.read_text(encoding="utf-8"),
    )
    print("Updated state-machine definition (Glue -> SNS, no recursion)")


# ============================================================
# MAIN
# ============================================================
def main():
    print("Deploying Lambda -> Step Functions orchestration ...")
    cfg = grant_lambda_start_sfn()
    update_lambda_code(cfg)
    update_state_machine()
    print(
        "\nDone. Upload a CSV to raw/ to trigger:"
        "\n  Lambda validate -> state machine -> Glue+DQ -> SNS."
    )


if __name__ == "__main__":
    main()
