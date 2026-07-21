"""
Deploy the local Glue ETL script to AWS and run the job -- from the editor.

The Glue job stores its code as a file in S3 (its "ScriptLocation"), not
inside the job definition. So updating the job = overwriting that S3 file.
This script:
    1. looks up the job's ScriptLocation,
    2. uploads glue_jobs/cms_transform.py there,
    3. starts a job run,
    4. polls until it finishes and prints the result.

No more copy-pasting into the Glue console.

Run:  python -m glue_jobs.deploy_and_run
Cost: a few Glue DPU-minutes (cents). It updates the live job + runs it.
"""

import time
from pathlib import Path

from config import settings
from config.aws_session import get_client

LOCAL_SCRIPT = Path(__file__).parent / "cms_transform.py"
_POLL_SECONDS = 15
_TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}


# ============================================================
# STEP 1 -- find where the job expects its script in S3
# ============================================================
def get_script_location() -> str:
    job = get_client("glue").get_job(JobName=settings.GLUE_JOB)
    return job["Job"]["Command"]["ScriptLocation"]


# ============================================================
# STEP 2 -- upload the local script to that S3 location
# ============================================================
def upload_script(s3_uri: str):
    _, _, rest = s3_uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    get_client("s3").upload_file(str(LOCAL_SCRIPT), bucket, key)
    print(f"Uploaded {LOCAL_SCRIPT.name} -> {s3_uri}")


# ============================================================
# STEP 3 -- run the job and wait
# ============================================================
def run_job() -> tuple[str, str]:
    glue = get_client("glue")
    run_id = glue.start_job_run(JobName=settings.GLUE_JOB)["JobRunId"]
    print(f"Started run {run_id} -- polling every {_POLL_SECONDS}s ...")

    while True:
        run = glue.get_job_run(JobName=settings.GLUE_JOB, RunId=run_id)["JobRun"]
        state = run["JobRunState"]
        if state in _TERMINAL:
            return run_id, state
        print(f"  {state} ...")
        time.sleep(_POLL_SECONDS)


# ============================================================
# MAIN
# ============================================================
def main():
    location = get_script_location()
    upload_script(location)

    run_id, state = run_job()
    print(f"\nRun {run_id} finished: {state}")
    if state != "SUCCEEDED":
        print("Check logs in the Glue console -> Runs -> this run.")
    else:
        print("Verify: dq-results/ has a JSON file and processed/ ~= 56 files.")


if __name__ == "__main__":
    main()
