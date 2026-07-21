"""
Milestone 4 (output) -- deploy the Athena gold layer.

Runs every .sql file in athena/gold/ (in filename order) against Athena to
(re)create the star-schema views and gold marts. Views are metadata only --
creating them costs nothing and scans no data.

Run from the VSCode terminal:  python -m athena.deploy_views

Idempotent: each file uses CREATE OR REPLACE VIEW, so re-running just updates.
"""

import time
from pathlib import Path

from config import settings
from config.aws_session import get_client

_GOLD_DIR = Path(__file__).parent / "gold"
_POLL_SECONDS = 2


# ============================================================
# Run a single statement and wait for it to finish
# ============================================================
def _run_query(sql: str) -> str:
    """Submit one SQL statement to Athena and block until it completes."""
    athena = get_client("athena")

    execution = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": settings.GLUE_DATABASE},
        WorkGroup=settings.ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": settings.ATHENA_OUTPUT_LOCATION},
    )
    qid = execution["QueryExecutionId"]

    # poll until SUCCEEDED / FAILED / CANCELLED
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(_POLL_SECONDS)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Query {qid} {state}: {reason}")
    return qid


# ============================================================
# MAIN -- deploy all gold views in order
# ============================================================
def main():
    sql_files = sorted(_GOLD_DIR.glob("*.sql"))
    if not sql_files:
        raise SystemExit(f"No .sql files found in {_GOLD_DIR}")

    print(f"Deploying {len(sql_files)} views to {settings.GLUE_DATABASE} ...")
    for path in sql_files:
        sql = path.read_text(encoding="utf-8")
        _run_query(sql)
        print(f"  [ok] {path.name}")

    print("\nGold layer deployed. Query e.g.:")
    print(f"  SELECT * FROM {settings.GLUE_DATABASE}.gold_cost_by_state LIMIT 10;")


if __name__ == "__main__":
    main()
