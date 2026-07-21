"""
CMS Claims Analytics -- Streamlit dashboard.

Reads the exported flat files (dashboard/exports/) produced by
export_gold_results.py (Athena gold views) and ml/score.py (model output).
No live AWS calls -- it opens instantly and costs nothing to view.

Run:  streamlit run dashboard/app.py

Refresh the data first with:
    python -m dashboard.export_gold_results
    python -m ml.score
"""

from pathlib import Path

import pandas as pd
import streamlit as st

EXPORTS = Path(__file__).parent / "exports"


# ============================================================
# Helpers -- load an export, or show a friendly hint if missing
# ============================================================
def _load(filename: str) -> pd.DataFrame | None:
    path = EXPORTS / filename
    if not path.exists():
        return None
    return pd.read_csv(path)


def _missing(name: str, how: str):
    st.info(f"`{name}` not found yet. Generate it with:\n\n`{how}`")


# ============================================================
# Page setup
# ============================================================
st.set_page_config(page_title="CMS Claims Analytics", layout="wide")
st.title("CMS Medicare Provider Analytics")
st.caption(
    "~1.3M real-world CMS provider records -> S3 / Glue / Athena gold layer, "
    "with an XGBoost high-cost risk model. Data is provider-level (not claims)."
)

tab_geo, tab_spec, tab_clin, tab_ml = st.tabs(
    ["Cost by State", "Cost by Specialty", "Chronic Conditions", "ML Risk Model"]
)


# ============================================================
# TAB 1 -- cost by state
# ============================================================
with tab_geo:
    st.subheader("Medicare payment by state")
    df = _load("cost_by_state.csv")
    if df is None:
        _missing("cost_by_state.csv", "python -m dashboard.export_gold_results")
    else:
        c1, c2 = st.columns(2)
        c1.metric("States", len(df))
        c2.metric("Total Medicare payment",
                  f"${df['total_medicare_payment'].sum():,.0f}")
        st.bar_chart(df.set_index("state")["total_medicare_payment"])
        st.dataframe(df, use_container_width=True)


# ============================================================
# TAB 2 -- cost by specialty
# ============================================================
with tab_spec:
    st.subheader("Highest average payment by provider type")
    df = _load("cost_by_specialty.csv")
    if df is None:
        _missing("cost_by_specialty.csv", "python -m dashboard.export_gold_results")
    else:
        top = df.head(15)
        st.bar_chart(top.set_index("provider_type")["avg_medicare_payment"])
        st.dataframe(df, use_container_width=True)


# ============================================================
# TAB 3 -- chronic conditions
# ============================================================
with tab_clin:
    st.subheader("Average chronic-condition prevalence by state")
    df = _load("chronic_conditions_by_state.csv")
    if df is None:
        _missing("chronic_conditions_by_state.csv",
                 "python -m dashboard.export_gold_results")
    else:
        condition = st.selectbox(
            "Condition",
            ["avg_pct_diabetes", "avg_pct_hypertension",
             "avg_pct_heart_failure", "avg_pct_ckd"],
        )
        st.bar_chart(df.set_index("state")[condition])
        st.dataframe(df, use_container_width=True)


# ============================================================
# TAB 4 -- ML risk model
# ============================================================
with tab_ml:
    st.subheader("High-cost provider risk model")

    preds = _load("predictions.csv")
    if preds is None:
        _missing("predictions.csv", "python -m ml.score")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Providers scored", f"{len(preds):,}")
        c2.metric("Flagged high-cost (actual)",
                  f"{int(preds['actual_high_cost'].sum()):,}")
        st.markdown("**Highest-risk providers**")
        st.dataframe(preds.head(50), use_container_width=True)

    fi = _load("feature_importance.csv")
    if fi is not None:
        st.markdown("**What drives high-cost risk (feature importance)**")
        st.bar_chart(fi.head(15).set_index("feature")["importance"])
