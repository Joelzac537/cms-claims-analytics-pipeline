"""
Publish the local ML predictions as an Athena/Glue table.

QuickSight reads from Athena, not local CSVs. This takes the scored
predictions (dashboard/exports/predictions.csv, written by ml.score) and
writes them to S3 as Parquet, registering a Glue table so they show up in
Athena + QuickSight alongside the gold views.

Run (after ml.score):  python -m ml.publish_predictions
Creates/refreshes table: cms_medical_db.gold_provider_risk
"""

from pathlib import Path

import awswrangler as wr
import pandas as pd

from config import settings
from config.aws_session import get_session

PRED_CSV = Path(__file__).parents[1] / "dashboard" / "exports" / "predictions.csv"
TABLE = "gold_provider_risk"


def main():
    if not PRED_CSV.exists():
        raise SystemExit(f"{PRED_CSV} not found -- run `python -m ml.score` first.")

    df = pd.read_csv(PRED_CSV)

    wr.s3.to_parquet(
        df=df,
        path=settings.s3_uri("ml/predictions/"),
        dataset=True,
        mode="overwrite",
        database=settings.GLUE_DATABASE,
        table=TABLE,
        boto3_session=get_session(),
    )
    print(f"Published {len(df):,} rows -> {settings.GLUE_DATABASE}.{TABLE}")
    print("Now available in Athena + QuickSight.")


if __name__ == "__main__":
    main()
