# dashboard_app.py
# CNC IIoT - Streamlit Dashboard (v2.0)
#
# Run:
#   python -m streamlit run dashboard_app.py
#
# Reads:
#   exports/jobs_summary.json
#   exports/job_<id>_kpis.json
#   exports/job_<id>_telemetry.csv

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt

BASE_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = BASE_DIR / "exports"
JOBS_SUMMARY_JSON = EXPORTS_DIR / "jobs_summary.json"


def load_json(path: Path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Path):
    if not path.exists():
        return None
    return pd.read_csv(path)


st.set_page_config(
    page_title="CNC IIoT Dashboard",
    layout="wide",
)

st.title("CNC IIoT Dashboard")
st.caption(f"Reading exports from: {EXPORTS_DIR}")

# ---- Load jobs summary ----
if not JOBS_SUMMARY_JSON.exists():
    st.error("Missing exports/jobs_summary.json. Run: python job_compare.py --export")
    st.stop()

jobs = load_json(JOBS_SUMMARY_JSON) or []
df = pd.DataFrame(jobs)

if df.empty:
    st.warning("No jobs found.")
    st.stop()

# ---- Sidebar ----
st.sidebar.header("Controls")
job_ids = df["job_id"].tolist()
selected_job_id = st.sidebar.selectbox("Select job", job_ids, index=0)

if st.sidebar.button("Refresh exports"):
    st.rerun()

# ---- Layout ----
top_left, top_right = st.columns([1.3, 0.7])
bottom_left, bottom_right = st.columns([1.0, 1.0])

# ---- Jobs table ----
with top_left:
    st.subheader("Jobs summary")

    display_cols = [
        "job_id", "job_name", "status", "material",
        "duration_human", "idle_pct", "alarm_pct",
        "efficiency_score_v2", "alarm_events"
    ]
    st.dataframe(df[display_cols], use_container_width=True)

# ---- Efficiency chart ----
with top_right:
    st.subheader("Efficiency per job")

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("job_id:O", title="Job ID"),
            y=alt.Y("efficiency_score_v2:Q", title="Efficiency (0–100)"),
            tooltip=["job_name", "efficiency_score_v2"]
        )
        .properties(height=250)
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Selected job details ----
kpis_path = EXPORTS_DIR / f"job_{selected_job_id}_kpis.json"
kpis = load_json(kpis_path)

with bottom_left:
    st.subheader(f"Job {selected_job_id} KPIs")

    if not kpis:
        st.warning("Run: python job_drilldown.py --export")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Status", kpis.get("status"))
        c2.metric("Duration", kpis.get("duration_human"))
        c3.metric("Efficiency", f"{kpis.get('efficiency_score_v2', 0):.1f} / 100")

        pie_df = pd.DataFrame({
            "State": ["Active", "Idle", "Alarm"],
            "Percent": [
                kpis.get("active_pct", 0),
                kpis.get("idle_pct", 0),
                kpis.get("alarm_pct", 0),
            ],
        })

        pie = (
            alt.Chart(pie_df)
            .mark_arc(innerRadius=40)
            .encode(
                theta="Percent:Q",
                color="State:N",
                tooltip=["State", "Percent"]
            )
            .properties(height=250)
        )

        st.subheader("Machine state distribution")
        st.altair_chart(pie, use_container_width=True)

# ---- Telemetry trend ----
with bottom_right:
    st.subheader("Feed rate trend")

    tel_path = EXPORTS_DIR / f"job_{selected_job_id}_telemetry.csv"
    tel_df = load_csv(tel_path)

    if tel_df is None or tel_df.empty:
        st.info("No telemetry CSV available")
    else:
        feed_col = None
        for c in ["feed", "feed_rate", "f"]:
            if c in tel_df.columns:
                feed_col = c
                break

        if feed_col is None:
            st.warning("No feed column found")
        else:
            tel_df["_sample"] = range(len(tel_df))
            line = (
                alt.Chart(tel_df)
                .mark_line()
                .encode(
                    x=alt.X("_sample:Q", title="Sample"),
                    y=alt.Y(f"{feed_col}:Q", title="Feed rate"),
                    tooltip=[feed_col]
                )
                .properties(height=250)
            )
            st.altair_chart(line, use_container_width=True)

st.divider()
st.caption("CNC IIoT Dashboard v2.0 • Data-driven job tracking & telemetry analysis")
