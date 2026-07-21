"""
CMS Medicare Provider Analytics -- executive BI dashboard (Streamlit).

Left sidebar navigation + branding, KPI cards, a US choropleth map, a donut,
and value-labelled charts in a grid. Light, patient-oriented healthcare look.

Reads lightweight CSV snapshots from dashboard/data/ (committed, so the
hosted app works) and falls back to dashboard/exports/ for local runs.

Refresh data:
    python -m dashboard.export_gold_results   # gold views -> exports/
    python -m ml.score                        # model output -> exports/
    python -m dashboard.build_dashboard_data  # exports/ -> committed data/

Run:  python -m streamlit run dashboard/app.py
"""

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

_DATA = Path(__file__).parent / "data"
_EXPORTS = Path(__file__).parent / "exports"

# ---- healthcare palette ----
TEAL, TEAL_DK, MINT, BLUE, SKY, CORAL, INK, MUTE = (
    "#159A93", "#0E7C77", "#57C4A5", "#3E8EDE", "#7FC8F8",
    "#F0876F", "#1F3A44", "#6B8088",
)

_US_TOPO = "https://cdn.jsdelivr.net/npm/vega-datasets@2/data/us-10m.json"

# 2-letter state -> FIPS id (matches the us-10m topojson ids)
STATE_FIPS = {
    "AL": 1, "AK": 2, "AZ": 4, "AR": 5, "CA": 6, "CO": 8, "CT": 9, "DE": 10,
    "DC": 11, "FL": 12, "GA": 13, "HI": 15, "ID": 16, "IL": 17, "IN": 18,
    "IA": 19, "KS": 20, "KY": 21, "LA": 22, "ME": 23, "MD": 24, "MA": 25,
    "MI": 26, "MN": 27, "MS": 28, "MO": 29, "MT": 30, "NE": 31, "NV": 32,
    "NH": 33, "NJ": 34, "NM": 35, "NY": 36, "NC": 37, "ND": 38, "OH": 39,
    "OK": 40, "OR": 41, "PA": 42, "RI": 44, "SC": 45, "SD": 46, "TN": 47,
    "TX": 48, "UT": 49, "VT": 50, "VA": 51, "WA": 53, "WV": 54, "WI": 55,
    "WY": 56,
}


# ============================================================
# DATA
# ============================================================
def load(filename: str):
    for base in (_DATA, _EXPORTS):
        if (base / filename).exists():
            return pd.read_csv(base / filename)
    return None


# ============================================================
# PAGE + THEME
# ============================================================
st.set_page_config(page_title="CMS Provider Analytics", page_icon="🩺", layout="wide")

st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito+Sans:opsz,wght@6..12,400;6..12,600;6..12,700;6..12,800&display=swap');
    html, body, [class*="css"], .stApp {{ font-family:'Nunito Sans',sans-serif; color:{INK}; }}
    .stApp {{ background:#EEF4F4; }}
    #MainMenu, footer, header {{ visibility:hidden; }}
    .block-container {{ padding-top:1.6rem; padding-bottom:2rem; max-width:1250px; }}

    /* sidebar */
    [data-testid="stSidebar"] {{ background:linear-gradient(180deg,{TEAL_DK} 0%,{TEAL} 100%); }}
    [data-testid="stSidebar"] * {{ color:#EAF6F5 !important; }}
    .brand {{ font-size:22px; font-weight:800; padding:6px 0 2px; }}
    .brand small {{ display:block; font-size:12px; font-weight:600; opacity:.8; }}

    /* page title */
    .ptitle {{ font-size:26px; font-weight:800; margin:0 0 2px; }}
    .psub   {{ color:{MUTE}; font-size:14px; margin:0 0 18px; }}

    /* KPI cards */
    .kpi {{ background:#fff; border-radius:16px; padding:18px 20px;
        box-shadow:0 3px 14px rgba(31,58,68,.07); border-top:4px solid {TEAL}; height:100%; }}
    .kpi .ico {{ font-size:20px; }}
    .kpi .label {{ font-size:12px; color:{MUTE}; font-weight:700; text-transform:uppercase; letter-spacing:.04em; margin-top:6px; }}
    .kpi .value {{ font-size:26px; font-weight:800; margin-top:2px; }}

    /* section card */
    .card {{ background:#fff; border-radius:16px; padding:18px 20px 8px;
        box-shadow:0 3px 14px rgba(31,58,68,.07); margin-bottom:8px; }}
    .card h4 {{ margin:0 0 10px; font-size:16px; font-weight:800; }}
    .foot {{ color:{MUTE}; font-size:12px; text-align:center; margin-top:18px; }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# COMPONENTS
# ============================================================
def kpi(col, icon, label, value, accent=TEAL):
    col.markdown(
        f'<div class="kpi" style="border-top-color:{accent}">'
        f'<div class="ico">{icon}</div><div class="label">{label}</div>'
        f'<div class="value">{value}</div></div>',
        unsafe_allow_html=True,
    )


def page_head(title, subtitle):
    st.markdown(f'<div class="ptitle">{title}</div>'
                f'<div class="psub">{subtitle}</div>', unsafe_allow_html=True)


def hbar(df, cat, val, color=TEAL, n=15):
    d = df.head(n)
    base = alt.Chart(d).encode(
        y=alt.Y(f"{cat}:N", sort="-x", title=None,
                axis=alt.Axis(labelLimit=220, labelColor=INK)),
        x=alt.X(f"{val}:Q", title=None, axis=alt.Axis(labels=False, grid=False)),
    )
    bars = base.mark_bar(color=color, cornerRadiusEnd=5)
    labels = base.mark_text(align="left", dx=4, color=MUTE, fontSize=11).encode(
        text=alt.Text(f"{val}:Q", format=",.0f"))
    return (bars + labels).properties(height=max(240, 26 * len(d))).configure_view(strokeWidth=0)


def choropleth(df, value, scheme="teals"):
    d = df.copy()
    d["id"] = d["state"].map(STATE_FIPS)
    d = d.dropna(subset=["id"])
    d["id"] = d["id"].astype(int)
    states = alt.topo_feature(_US_TOPO, "states")
    return (
        alt.Chart(states)
        .mark_geoshape(stroke="white", strokeWidth=0.6)
        .transform_lookup(lookup="id", from_=alt.LookupData(d, "id", ["state", value]))
        .encode(
            color=alt.Color(f"{value}:Q", scale=alt.Scale(scheme=scheme),
                            legend=alt.Legend(orient="bottom", title=None, gradientLength=220)),
            tooltip=[alt.Tooltip("state:N", title="State"),
                     alt.Tooltip(f"{value}:Q", title="Value", format=",.0f")],
        )
        .project(type="albersUsa")
        .properties(height=430)
        .configure_view(strokeWidth=0)
    )


def donut(part, whole, label):
    d = pd.DataFrame({
        "cat": [label, "Other"],
        "n": [part, max(whole - part, 0)],
    })
    ring = (
        alt.Chart(d)
        .mark_arc(innerRadius=62, cornerRadius=3)
        .encode(
            theta="n:Q",
            color=alt.Color("cat:N", scale=alt.Scale(range=[CORAL, "#E3ECEC"]),
                            legend=None),
            tooltip=["cat:N", "n:Q"],
        )
        .properties(height=220)
    )
    return ring


def missing(hint):
    st.info(f"Data not generated yet. Run: `{hint}`")


# ============================================================
# SIDEBAR NAV
# ============================================================
with st.sidebar:
    st.markdown('<div class="brand">🩺 CMS Analytics'
                '<small>Medicare provider intelligence</small></div>',
                unsafe_allow_html=True)
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["Overview", "Geographic", "Specialty", "Chronic conditions", "Risk model"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption("Source: CMS Medicare Physician & Other Practitioners "
               "(~1.3M providers). Pipeline: S3 · Glue · Athena · XGBoost.")

state = load("cost_by_state.csv")
spec = load("cost_by_specialty.csv")
chronic = load("chronic_conditions_by_state.csv")
summary = load("predictions_summary.csv")
preds = load("predictions_top.csv")
if preds is None:
    preds = load("predictions.csv")
fi = load("feature_importance.csv")


# ============================================================
# PAGES
# ============================================================
def overview():
    page_head("Overview", "Cost, utilization and risk across U.S. Medicare providers")
    if state is not None:
        c1, c2, c3, c4 = st.columns(4)
        kpi(c1, "🏥", "Total Medicare $",
            f"${state['total_medicare_payment'].sum()/1e9:.1f}B", TEAL)
        kpi(c2, "👥", "Beneficiaries",
            f"{state['total_beneficiaries'].sum()/1e6:.1f}M", BLUE)
        kpi(c3, "📍", "States", f"{len(state)}", MINT)
        flagged = int(summary['flagged'][0]) if summary is not None else 0
        kpi(c4, "⚠️", "High-cost flagged", f"{flagged:,}", CORAL)
        st.write("")
        left, right = st.columns([1.3, 1])
        with left:
            st.markdown('<div class="card"><h4>Medicare payment by state</h4>',
                        unsafe_allow_html=True)
            st.altair_chart(choropleth(state, "total_medicare_payment"),
                            use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with right:
            if spec is not None:
                st.markdown('<div class="card"><h4>Top specialties by avg payment</h4>',
                            unsafe_allow_html=True)
                st.altair_chart(hbar(spec, "provider_type", "avg_medicare_payment", BLUE, 8),
                                use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
    else:
        missing("python -m dashboard.export_gold_results")


def geographic():
    page_head("Geographic", "Where Medicare spending concentrates")
    if state is None:
        missing("python -m dashboard.export_gold_results"); return
    metric = st.selectbox(
        "Metric",
        ["total_medicare_payment", "avg_medicare_payment", "total_beneficiaries"],
        format_func=lambda s: s.replace("_", " ").title())
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.altair_chart(choropleth(state, metric), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.dataframe(state, use_container_width=True, hide_index=True)


def specialty():
    page_head("Specialty", "Provider types driving the highest payments")
    if spec is None:
        missing("python -m dashboard.export_gold_results"); return
    st.markdown('<div class="card"><h4>Top 15 by average Medicare payment</h4>',
                unsafe_allow_html=True)
    st.altair_chart(hbar(spec, "provider_type", "avg_medicare_payment", BLUE),
                    use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.dataframe(spec, use_container_width=True, hide_index=True)


def chronic_page():
    page_head("Chronic conditions", "Average prevalence across provider panels")
    if chronic is None:
        missing("python -m dashboard.export_gold_results"); return
    labels = {"avg_pct_diabetes": "Diabetes", "avg_pct_hypertension": "Hypertension",
              "avg_pct_heart_failure": "Heart failure", "avg_pct_ckd": "Chronic kidney disease"}
    choice = st.selectbox("Condition", list(labels), format_func=labels.get)
    st.markdown(f'<div class="card"><h4>{labels[choice]} prevalence by state (%)</h4>',
                unsafe_allow_html=True)
    st.altair_chart(choropleth(chronic, choice, scheme="tealblues"),
                    use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def risk_page():
    page_head("Risk model", "XGBoost high-cost-provider classifier")
    if preds is None:
        missing("python -m ml.score"); return
    total = int(summary['total'][0]) if summary is not None else len(preds)
    flagged = int(summary['flagged'][0]) if summary is not None \
        else int(preds['actual_high_cost'].sum())
    left, right = st.columns([1, 1.4])
    with left:
        c1, c2 = st.columns(2)
        kpi(c1, "🧮", "Scored", f"{total:,}", TEAL)
        kpi(c2, "⚠️", "High-cost", f"{flagged:,}", CORAL)
        st.markdown('<div class="card"><h4>High-cost share</h4>', unsafe_allow_html=True)
        st.altair_chart(donut(flagged, total, "High-cost"), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        if fi is not None:
            st.markdown('<div class="card"><h4>What drives high-cost risk</h4>',
                        unsafe_allow_html=True)
            st.altair_chart(hbar(fi, "feature", "importance", CORAL, 12),
                            use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("#### Highest-risk providers")
    st.dataframe(preds.head(50), use_container_width=True, hide_index=True)


{
    "Overview": overview, "Geographic": geographic, "Specialty": specialty,
    "Chronic conditions": chronic_page, "Risk model": risk_page,
}[page]()

st.markdown('<div class="foot">Built on AWS · S3 → Glue/PySpark → Athena gold layer '
            '→ XGBoost · Streamlit</div>', unsafe_allow_html=True)
