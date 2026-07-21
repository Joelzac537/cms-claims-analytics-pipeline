"""
CMS Medicare Provider Analytics -- Streamlit dashboard.

A light, patient-oriented healthcare UI (calming teal/green, soft cards)
over the pipeline's gold layer + XGBoost risk model.

Reads lightweight CSV snapshots from dashboard/data/ (committed, so the
hosted app works) and falls back to dashboard/exports/ for local runs.

Refresh the data with:
    python -m dashboard.export_gold_results   # gold views -> exports/
    python -m ml.score                        # model output -> exports/
    python -m dashboard.build_dashboard_data  # exports/ -> committed data/

Run:  streamlit run dashboard/app.py
"""

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# committed snapshot first (for hosting), else local exports
_DATA = Path(__file__).parent / "data"
_EXPORTS = Path(__file__).parent / "exports"

# ---- healthcare palette ----
TEAL, MINT, BLUE, SKY, CORAL, INK = (
    "#159A93", "#57C4A5", "#3E8EDE", "#7FC8F8", "#F0876F", "#1F3A44"
)


# ============================================================
# DATA LOADING
# ============================================================
def load(filename: str):
    for base in (_DATA, _EXPORTS):
        path = base / filename
        if path.exists():
            return pd.read_csv(path)
    return None


# ============================================================
# PAGE + THEME (CSS)
# ============================================================
st.set_page_config(
    page_title="CMS Provider Analytics",
    page_icon="🩺",
    layout="wide",
)

st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito+Sans:opsz,wght@6..12,400;6..12,700;6..12,800&display=swap');

    html, body, [class*="css"], .stApp {{
        font-family: 'Nunito Sans', sans-serif;
        color: {INK};
    }}
    .stApp {{ background: {"#F3F8F8"}; }}

    /* hide default chrome */
    #MainMenu, footer, header {{ visibility: hidden; }}

    /* hero header */
    .hero {{
        background: linear-gradient(135deg, {TEAL} 0%, {BLUE} 100%);
        padding: 26px 32px; border-radius: 18px; color: #fff;
        margin-bottom: 22px; box-shadow: 0 6px 20px rgba(21,154,147,0.25);
    }}
    .hero h1 {{ margin: 0; font-size: 30px; font-weight: 800; color:#fff; }}
    .hero p  {{ margin: 6px 0 0; font-size: 15px; opacity: 0.92; }}

    /* KPI cards */
    .kpi {{
        background: #fff; border-radius: 14px; padding: 18px 20px;
        border-left: 6px solid {TEAL};
        box-shadow: 0 2px 10px rgba(31,58,68,0.06);
    }}
    .kpi .label {{ font-size: 13px; color: #6b8088; font-weight: 700;
        text-transform: uppercase; letter-spacing: .04em; }}
    .kpi .value {{ font-size: 28px; font-weight: 800; color: {INK}; margin-top: 4px; }}

    /* tabs */
    .stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
    .stTabs [data-baseweb="tab"] {{
        background: #E8F3F2; border-radius: 10px 10px 0 0;
        padding: 10px 18px; font-weight: 700; color: {INK};
    }}
    .stTabs [aria-selected="true"] {{ background: {TEAL}; color: #fff; }}

    .block-container {{ padding-top: 2rem; max-width: 1200px; }}
    </style>
    """,
    unsafe_allow_html=True,
)


def hero():
    st.markdown(
        """
        <div class="hero">
            <h1>🩺 CMS Medicare Provider Analytics</h1>
            <p>Cost, utilization &amp; chronic-condition insights across ~1.3M
            U.S. Medicare providers — with an XGBoost high-cost risk model.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi(col, label: str, value: str, accent: str = TEAL):
    col.markdown(
        f'<div class="kpi" style="border-left-color:{accent}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div></div>',
        unsafe_allow_html=True,
    )


def bar(df, cat, val, color=TEAL, horizontal=True):
    """A clean, rounded Altair bar chart with tooltips."""
    enc_cat = alt.Y(f"{cat}:N", sort="-x", title=None) if horizontal \
        else alt.X(f"{cat}:N", sort="-y", title=None)
    enc_val = alt.X(f"{val}:Q", title=None) if horizontal \
        else alt.Y(f"{val}:Q", title=None)
    return (
        alt.Chart(df)
        .mark_bar(color=color, cornerRadius=5)
        .encode(x=enc_val, y=enc_cat, tooltip=list(df.columns))
        .properties(height=380)
        .configure_view(strokeWidth=0)
        .configure_axis(labelColor=INK, labelFontSize=12, grid=False)
    )


def missing(hint: str):
    st.info(f"Data not generated yet. Run: `{hint}`")


# ============================================================
# LAYOUT
# ============================================================
hero()

tab_geo, tab_spec, tab_clin, tab_ml = st.tabs(
    ["🗺️  By State", "🩻  By Specialty", "❤️  Chronic Conditions", "🤖  Risk Model"]
)

# ---- Tab 1: cost by state ----
with tab_geo:
    df = load("cost_by_state.csv")
    if df is None:
        missing("python -m dashboard.export_gold_results")
    else:
        c1, c2, c3 = st.columns(3)
        kpi(c1, "States", f"{len(df)}")
        kpi(c2, "Total Medicare $", f"${df['total_medicare_payment'].sum()/1e9:.1f}B", BLUE)
        kpi(c3, "Beneficiaries", f"{df['total_beneficiaries'].sum()/1e6:.1f}M", MINT)
        st.markdown("#### Total Medicare payment by state")
        st.altair_chart(
            bar(df.head(20), "state", "total_medicare_payment", TEAL),
            use_container_width=True,
        )
        with st.expander("See full table"):
            st.dataframe(df, use_container_width=True, hide_index=True)

# ---- Tab 2: cost by specialty ----
with tab_spec:
    df = load("cost_by_specialty.csv")
    if df is None:
        missing("python -m dashboard.export_gold_results")
    else:
        kpi(st.columns(3)[0], "Provider types", f"{len(df)}", BLUE)
        st.markdown("#### Highest average Medicare payment by provider type")
        st.altair_chart(
            bar(df.head(15), "provider_type", "avg_medicare_payment", BLUE),
            use_container_width=True,
        )
        with st.expander("See full table"):
            st.dataframe(df, use_container_width=True, hide_index=True)

# ---- Tab 3: chronic conditions ----
with tab_clin:
    df = load("chronic_conditions_by_state.csv")
    if df is None:
        missing("python -m dashboard.export_gold_results")
    else:
        labels = {
            "avg_pct_diabetes": "Diabetes",
            "avg_pct_hypertension": "Hypertension",
            "avg_pct_heart_failure": "Heart failure",
            "avg_pct_ckd": "Chronic kidney disease",
        }
        choice = st.selectbox("Condition", list(labels), format_func=labels.get)
        st.markdown(f"#### Average {labels[choice].lower()} prevalence by state")
        st.altair_chart(bar(df, "state", choice, MINT), use_container_width=True)
        with st.expander("See full table"):
            st.dataframe(df, use_container_width=True, hide_index=True)

# ---- Tab 4: ML risk model ----
with tab_ml:
    summary = load("predictions_summary.csv")
    preds = load("predictions_top.csv")
    if preds is None:
        preds = load("predictions.csv")  # local full file fallback

    if preds is None:
        missing("python -m ml.score")
    else:
        total = int(summary["total"][0]) if summary is not None else len(preds)
        flagged = int(summary["flagged"][0]) if summary is not None \
            else int(preds["actual_high_cost"].sum())
        c1, c2 = st.columns(2)
        kpi(c1, "Providers scored", f"{total:,}")
        kpi(c2, "Flagged high-cost", f"{flagged:,}", CORAL)

        st.markdown("#### Highest-risk providers")
        st.dataframe(preds.head(50), use_container_width=True, hide_index=True)

    fi = load("feature_importance.csv")
    if fi is not None:
        st.markdown("#### What drives high-cost risk")
        st.altair_chart(bar(fi.head(15), "feature", "importance", CORAL),
                        use_container_width=True)
