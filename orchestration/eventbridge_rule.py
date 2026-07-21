"""
Milestone 6 -- the missing trigger.

Current state of the pipeline:
    * A Step Functions state machine ("cms-pipeline-orchestrator") already
      exists and runs: Glue ETL -> quality-check Lambda -> SNS notify.
    * BUT nothing starts it automatically. Today a run is kicked off by hand.

What this script wires up:
    S3 (new file in raw/)  ->  EventBridge rule  ->  Step Functions execution

That completes the "EventBridge -> Step Functions -> SNS" milestone: dropping
a CSV into s3://<bucket>/raw/ automatically launches the whole pipeline.

This is a DEPLOY script -- it CREATES/UPDATES AWS resources (an IAM role, an
EventBridge rule, an S3 notification setting). Run it yourself from the VSCode
terminal only when you're ready:  python -m orchestration.eventbridge_rule
It is idempotent: re-running it updates in place rather than duplicating.
"""

import json

from config import settings
from config.aws_session import get_client

# EventBridge needs permission to assume a role that can start the state machine.
_ROLE_NAME = "cms-eventbridge-invoke-sfn"


# ============================================================
# STEP 1 -- IAM role that EventBridge assumes to start the state machine
# ============================================================
def ensure_invoke_role() -> str:
    """Create (or reuse) an IAM role that lets EventBridge start the SFN."""
    iam = get_client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    try:
        role = iam.create_role(
            RoleName=_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Lets EventBridge start the CMS pipeline state machine",
        )
        role_arn = role["Role"]["Arn"]
        print(f"Created role {_ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=_ROLE_NAME)["Role"]["Arn"]
        print(f"Reusing existing role {_ROLE_NAME}")

    # Inline policy: allow starting ONLY our specific state machine (least privilege)
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": settings.STATE_MACHINE_ARN,
        }],
    }
    iam.put_role_policy(
        RoleName=_ROLE_NAME,
        PolicyName="start-cms-state-machine",
        PolicyDocument=json.dumps(permission_policy),
    )
    print("Attached least-privilege StartExecution policy")
    return role_arn


# ============================================================
# STEP 2 -- turn on EventBridge notifications for the bucket
# ============================================================
def enable_bucket_eventbridge():
    """S3 only emits events to EventBridge when this is switched on."""
    s3 = get_client("s3")
    s3.put_bucket_notification_configuration(
        Bucket=settings.S3_BUCKET,
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )
    print(f"EventBridge notifications enabled on s3://{settings.S3_BUCKET}")


# ============================================================
# STEP 3 -- the rule: match new objects under raw/
# ============================================================
def create_rule() -> str:
    """Create/update the EventBridge rule for raw/ uploads."""
    events = get_client("events")

    event_pattern = {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {"name": [settings.S3_BUCKET]},
            "object": {"key": [{"prefix": settings.RAW_PREFIX}]},
        },
    }

    rule = events.put_rule(
        Name=settings.EVENTBRIDGE_RULE_NAME,
        EventPattern=json.dumps(event_pattern),
        State="ENABLED",
        Description="Start CMS pipeline when a CSV lands in raw/",
    )
    print(f"Rule ready: {settings.EVENTBRIDGE_RULE_NAME}")
    return rule["RuleArn"]


# ============================================================
# STEP 4 -- point the rule at the Step Functions state machine
# ============================================================
def add_state_machine_target(role_arn: str):
    """Attach the state machine as the rule's target."""
    events = get_client("events")
    events.put_targets(
        Rule=settings.EVENTBRIDGE_RULE_NAME,
        Targets=[{
            "Id": "cms-state-machine",
            "Arn": settings.STATE_MACHINE_ARN,
            "RoleArn": role_arn,
        }],
    )
    print("State machine attached as target -- orchestration is now automatic")


# ============================================================
# MAIN -- run the four steps in order
# ============================================================
def main():
    print("Wiring EventBridge -> Step Functions ...")
    role_arn = ensure_invoke_role()
    enable_bucket_eventbridge()
    create_rule()
    add_state_machine_target(role_arn)
    print("\nDone. Upload a CSV to raw/ to trigger the pipeline end-to-end.")


if __name__ == "__main__":
    main()
