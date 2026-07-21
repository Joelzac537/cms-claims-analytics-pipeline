"""
Dashboard step 1 -- export the Athena gold views to local CSVs.

The Streamlit app reads flat files (not live Athena) so it starts instantly
and costs nothing to view. This script refreshes those files by querying each
gold view once and writing it to dashboard/exports/.

Run:  python -m dashboard.export_gold_results
(Run ml.score too, to refresh the ML predictions export.)
"""

from pathlib import Path

import awswrangler as wr

from config import settings
from config.aws_session import get_session

EXPORTS = Path(__file__).parent / "exports"

# view name -> export filename
_GOLD_VIEWS = {
    "gold_cost_by_state": "cost_by_state.csv",
    "gold_cost_by_specialty": "cost_by_specialty.csv",
    "gold_high_cost_providers": "high_cost_providers.csv",
    "gold_chronic_conditions_by_state": "chronic_conditions_by_state.csv",
}


def main():
    EXPORTS.mkdir(parents=True, exist_ok=True)
    session = get_session()

    for view, filename in _GOLD_VIEWS.items():
        df = wr.athena.read_sql_query(
            f"SELECT * FROM {view}",
            database=settings.GLUE_DATABASE,
            workgroup=settings.ATHENA_WORKGROUP,
            s3_output=settings.ATHENA_OUTPUT_LOCATION,
            boto3_session=session,
        )
        df.to_csv(EXPORTS / filename, index=False)
        print(f"  [ok] {view} -> {filename} ({len(df):,} rows)")

    print(f"\nGold results exported to {EXPORTS}")


if __name__ == "__main__":
    main()
