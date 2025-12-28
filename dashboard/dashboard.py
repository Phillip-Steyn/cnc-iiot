import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


# ---------- Setup ----------
st.set_page_config(page_title="CNC IIoT Dashboard", layout="wide")
st.title("CNC IIoT Dashboard")

DB_PATH = Path(__file__).resolve().parents[1] / "cnc_iiot.db"
st.caption(f"DB path: {DB_PATH}")

if not DB_PATH.exists():
    st.error("cnc_iiot.db not found in project root (CNC_IIOT).")
    st.stop()

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto refresh (3s)", value=True)
    show_raw = st.toggle("Show Telemetry + Events", value=False)

# Optional auto refresh
if auto_refresh:
    st.caption("🔄 Auto refresh enabled")
    st.cache_data.clear()


@st.cache_data(ttl=3)
def load_data(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        jobs = pd.read_sql_query("SELECT * FROM jobs ORDER BY id DESC", conn)
        telemetry = pd.read_sql_query("SELECT * FROM telemetry ORDER BY id DESC LIMIT 200", conn)
        events = pd.read_sql_query("SELECT * FROM events ORDER BY id DESC LIMIT 200", conn)
    finally:
        conn.close()
    return jobs, telemetry, events


def to_dt(s):
    return pd.to_datetime(s, errors="coerce", utc=True)


def status_badge(status: str) -> str:
    s = (status or "").strip().lower()
    if s == "running":
        return "🟢 running"
    if s == "finished":
        return "✅ finished"
    if s == "created":
        return "🟡 created"
    if s in ("error", "failed"):
        return "🔴 error"
    return f"⚪ {status}"


def fmt_duration(sec):
    if sec is None:
        return "—"
    sec = int(max(sec, 0))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


jobs, telemetry, events = load_data(DB_PATH)

if jobs.empty:
    st.warning("No jobs found in DB yet.")
    st.stop()

# ---------- Pick active job ----------
running = jobs[jobs["status"].astype(str).str.lower() == "running"]
active_job = running.iloc[0] if not running.empty else jobs.iloc[0]

job_id = int(active_job["id"])
job_name = str(active_job.get("job_name", ""))
status = str(active_job.get("status", ""))
material = str(active_job.get("material", ""))
notes = str(active_job.get("notes", ""))

created = to_dt(active_job.get("created_ts_utc"))
started = to_dt(active_job.get("started_ts_utc"))
finished = to_dt(active_job.get("finished_ts_utc"))

now_utc = pd.Timestamp.utcnow()

duration_seconds = None
if pd.notna(started):
    end_t = finished if pd.notna(finished) else now_utc
    duration_seconds = (end_t - started).total_seconds()

# ---------- Header summary ----------
st.success("App is running", icon="✅")

st.subheader("Overview")

c1, c2, c3, c4 = st.columns([2.2, 1, 1, 1])

with c1:
    # A cleaner "card" using markdown + spacing
    st.markdown("### Active Job")
    st.markdown(f"**ID:** `{job_id}`")
    st.markdown(f"**Name:** {job_name}")
    st.markdown(f"**Status:** {status_badge(status)}")
    if material and material != "None":
        st.markdown(f"**Material:** {material}")
    if notes and notes != "None":
        st.markdown(f"**Notes:** {notes}")

    # Timestamps (compact)
    ts_line = []
    if pd.notna(started):
        ts_line.append(f"Started: `{started.isoformat()}`")
    if pd.notna(finished):
        ts_line.append(f"Finished: `{finished.isoformat()}`")
    if ts_line:
        st.caption(" | ".join(ts_line))

with c2:
    st.metric("Jobs total", len(jobs))

with c3:
    st.metric("Last job status", status_badge(str(jobs.iloc[0].get("status", ""))))

with c4:
    st.metric("Active duration", fmt_duration(duration_seconds))

st.divider()

# ---------- Jobs table (polished) ----------
st.subheader("Jobs")

jobs_view = jobs.copy()
# Add a pretty status column
jobs_view["status"] = jobs_view["status"].astype(str).apply(status_badge)

# Reorder columns (nice)
preferred_cols = ["id", "job_name", "status", "material", "created_ts_utc", "started_ts_utc", "finished_ts_utc", "notes"]
cols = [c for c in preferred_cols if c in jobs_view.columns] + [c for c in jobs_view.columns if c not in preferred_cols]
jobs_view = jobs_view[cols]

# Highlight active job row
def highlight_active(row):
    return ["background-color: rgba(0, 255, 0, 0.08)"] * len(row) if int(row["id"]) == job_id else [""] * len(row)

st.dataframe(
    jobs_view.style.apply(highlight_active, axis=1),
    use_container_width=True,
    hide_index=True,
)

# ---------- Optional raw panels ----------
if show_raw:
    st.subheader("Details")

    with st.expander("Recent Telemetry (latest 200)", expanded=False):
        st.dataframe(telemetry, use_container_width=True, hide_index=True)

    with st.expander("Recent Events (latest 200)", expanded=False):
        st.dataframe(events, use_container_width=True, hide_index=True)
