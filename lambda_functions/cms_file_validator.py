"""
S3-triggered entry point for the CMS pipeline.

Flow:  new CSV in raw/  ->  (this Lambda)  ->  Step Functions state machine

This Lambda validates the uploaded file, then STARTS the state machine
(cms-pipeline-orchestrator), which runs the Glue ETL + data-quality job and
sends SNS notifications. It deliberately does NOT start Glue directly --
orchestration is the state machine's job now.
"""

import json
import boto3

# ============================================================
# CONFIG
# ============================================================
# The orchestration state machine this validator kicks off.
STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-2:933022095648:stateMachine:cms-pipeline-orchestrator"
)

sfn = boto3.client("stepfunctions", region_name="us-east-2")


# ============================================================
# HANDLER
# ============================================================
def lambda_handler(event, context):
    # ---- read the S3 event ----
    s3 = event["Records"][0]["s3"]
    bucket = s3["bucket"]["name"]
    key = s3["object"]["key"]
    size = s3["object"]["size"]
    print(f"File received: {key} in {bucket} ({size} bytes)")

    # ---- validate ----
    if not key.endswith(".csv"):
        print(f"ERROR: {key} is not a CSV file!")
        return {"statusCode": 400, "body": json.dumps(f"Invalid file type: {key}")}

    if size == 0:
        print(f"ERROR: {key} is empty!")
        return {"statusCode": 400, "body": json.dumps(f"Empty file: {key}")}

    print(f"Validation passed for {key}")

    # ---- start the orchestration ----
    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps({"bucket": bucket, "key": key}),
    )
    execution_arn = response["executionArn"]
    print(f"Started state machine: {execution_arn}")

    return {
        "statusCode": 200,
        "body": json.dumps(f"Validation passed. Pipeline started for {key}"),
    }
