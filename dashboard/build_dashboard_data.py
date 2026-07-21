"""
Assemble a small, committable dashboard snapshot from the local exports.

The hosted Streamlit app can't call AWS, so it ships with a snapshot of the
data in dashboard/data/ (committed to git). This script builds that snapshot
from dashboard/exports/ (produced by export_gold_results + ml.score):

  * copies the tiny gold-view CSVs + feature_importance as-is,
  * trims the 1.26M-row predictions down to the top-risk rows the UI shows,
  * writes a one-row predictions_summary so the KPI cards stay accurate.

Run (after export_gold_results + ml.score):
    python -m dashboard.build_dashboard_data
"""

from pathlib import Path

import pandas as pd

_EXPORTS = Path(__file__).parent / "exports"
_DATA = Path(__file__).parent / "data"

# small files copied straight through
_COPY = [
    "cost_by_state.csv",
    "cost_by_specialty.csv",
    "chronic_conditions_by_state.csv",
    "feature_importance.csv",
]
_TOP_N = 500


def main():
    _DATA.mkdir(parents=True, exist_ok=True)

    for name in _COPY:
        src = _EXPORTS / name
        if src.exists():
            (_DATA / name).write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"  copied {name}")
        else:
            print(f"  [skip] {name} not found -- run export_gold_results")

    preds_path = _EXPORTS / "predictions.csv"
    if preds_path.exists():
        preds = pd.read_csv(preds_path)
        # summary keeps the true totals even though we only ship the top rows
        pd.DataFrame([{
            "total": len(preds),
            "flagged": int(preds["actual_high_cost"].sum()),
        }]).to_csv(_DATA / "predictions_summary.csv", index=False)
        preds.head(_TOP_N).to_csv(_DATA / "predictions_top.csv", index=False)
        print(f"  trimmed predictions -> top {_TOP_N} + summary")
    else:
        print("  [skip] predictions.csv not found -- run ml.score")

    print(f"\nDashboard snapshot ready in {_DATA} (commit this for hosting).")


if __name__ == "__main__":
    main()
