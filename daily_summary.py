# daily_summary.py
# CNC IIoT - Daily Summary Report (v1.2)
#
# Default behavior: last 7 days (UTC).
# Duration fallback: if jobs timestamps show 0s, use telemetry span (and sim fallback 1s/sample).
#
# Usage:
#   python daily_summary.py
#   python daily_summary.py --days 1
#   python daily_summary.py --date 2025-12-27
#   python daily_summary.py --days 7 --export
#   python daily_summary.py --days 30 --export --db "C:\path\to\cnc_iiot.db"

import argparse
import os
import sqlite3
from datetime import datetime, date, timedelta, UTC
from typing import Optional, Any, Dict, List

DB_PATH_DEFAULT = "cnc_iiot.db"


def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s2 = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s2)
    except ValueError:
        return None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def human_secs(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}h {m}m {sec}s"
    if m > 0:
        return f"{m}m {sec}s"
    return f"{sec}s"


def compute_duration(start_iso: Optional[str], end_iso: Optional[str]) -> float:
    sdt = iso_to_dt(start_iso)
    edt = iso_to_dt(end_iso)
    if not sdt or not edt:
        return 0.0
    return max(0.0, (edt - sdt).total_seconds())


def fetch_jobs(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, job_name, status, material, notes,
               created_ts_utc, started_ts_utc, finished_ts_utc
        FROM jobs
        ORDER BY id ASC
        """
    ).fetchall()

    cols = ["id", "job_name", "status", "material", "notes", "created_ts_utc", "started_ts_utc", "finished_ts_utc"]
    return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]


def filter_jobs_by_date_range(jobs: List[Dict[str, Any]], start_date: date, end_date: date) -> List[Dict[str, Any]]:
    out = []
    for j in jobs:
        created = iso_to_dt(j.get("created_ts_utc"))
        if not created:
            continue
        d = created.date()
        if start_date <= d <= end_date:
            out.append(j)
    return out


def count_alarm_events(conn: sqlite3.Connection, job_id: int) -> int:
    cur = conn.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(events)").fetchall()]
    lower = {c.lower(): c for c in cols}

    job_col = lower.get("job_id")
    type_col = lower.get("event_type") or lower.get("type") or lower.get("level") or lower.get("category")
    msg_col = lower.get("message") or lower.get("msg") or lower.get("detail") or lower.get("details") or lower.get("text")

    if not job_col:
        return 0

    q = f"SELECT * FROM events WHERE {job_col} = ?"
    rows = cur.execute(q, (job_id,)).fetchall()
    if not rows:
        return 0

    desc = [d[0] for d in cur.description]
    alarms = 0
    for r in rows:
        d = {desc[i]: r[i] for i in range(len(desc))}
        et = str(d.get(type_col) or "").lower() if type_col else ""
        msg = str(d.get(msg_col) or "").lower() if msg_col else ""
        if "alarm" in et or "alarm" in msg:
            alarms += 1
    return alarms


def telemetry_span_seconds(conn: sqlite3.Connection, job_id: int, sample_interval_s_default: float = 1.0) -> float:
    """Best-effort: compute telemetry span for a job.
    If timestamps are identical, fall back to (samples-1) * sample_interval_s_default.
    """
    cur = conn.cursor()
    cols = [r[1] for r in cur.execute("PRAGMA table_info(telemetry)").fetchall()]
    lower = {c.lower(): c for c in cols}

    job_col = lower.get("job_id")
    ts_col = lower.get("ts_utc") or lower.get("timestamp_utc") or lower.get("timestamp") or lower.get("ts")

    if not job_col or not ts_col:
        return 0.0

    rows = cur.execute(
        f"SELECT {ts_col} FROM telemetry WHERE {job_col} = ? ORDER BY {ts_col} ASC",
        (job_id,),
    ).fetchall()

    if not rows:
        return 0.0

    samples = len(rows)
    first_ts = rows[0][0]
    last_ts = rows[-1][0]

    span = compute_duration(first_ts, last_ts)
    if span == 0.0 and samples > 1:
        span = (samples - 1) * sample_interval_s_default
    return span


def main():
    parser = argparse.ArgumentParser(description="CNC IIoT Daily Summary Report (UTC)")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB (default: cnc_iiot.db)")
    parser.add_argument("--date", help="UTC date in YYYY-MM-DD (overrides --days and produces 1-day report)")
    parser.add_argument("--days", type=int, default=7, help="Number of days ending today UTC (default: 7)")
    parser.add_argument("--export", action="store_true", help="Write report to ./exports")
    args = parser.parse_args()

    today_utc = datetime.now(UTC).date()

    if args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        end_date = start_date
    else:
        days = max(1, int(args.days))
        end_date = today_utc
        start_date = end_date - timedelta(days=days - 1)

    conn = sqlite3.connect(args.db)

    jobs_all = fetch_jobs(conn)
    jobs = filter_jobs_by_date_range(jobs_all, start_date, end_date)

    finished = sum(1 for j in jobs if str(j.get("status") or "").lower() == "finished")
    created = sum(1 for j in jobs if str(j.get("status") or "").lower() == "created")
    started = sum(1 for j in jobs if str(j.get("status") or "").lower() == "started")
    other = len(jobs) - (finished + created + started)

    total_alarm_events = 0
    durations = []

    # Build a per-job duration using timestamp duration, with telemetry fallback
    per_job_duration = {}

    for j in jobs:
        jid = int(j["id"])
        total_alarm_events += count_alarm_events(conn, jid)

        dur = compute_duration(j.get("started_ts_utc"), j.get("finished_ts_utc"))

        if dur == 0.0:
            # fallback to telemetry span
            dur = telemetry_span_seconds(conn, jid, sample_interval_s_default=1.0)

        per_job_duration[jid] = dur
        durations.append(dur)

    avg_duration = sum(durations) / len(durations) if durations else 0.0

    lines = []
    lines.append("=" * 58)
    lines.append(" CNC IIoT - DAILY SUMMARY REPORT (v1.2)")
    lines.append("=" * 58)
    if start_date == end_date:
        lines.append(f"Date (UTC): {start_date.isoformat()}")
    else:
        lines.append(f"Range (UTC): {start_date.isoformat()} to {end_date.isoformat()}")
    lines.append("")
    lines.append(f"Jobs total        : {len(jobs)}")
    lines.append(f"Jobs finished     : {finished}")
    lines.append(f"Jobs started      : {started}")
    lines.append(f"Jobs created      : {created}")
    lines.append(f"Jobs other        : {other}")
    lines.append("")
    lines.append(f"Alarm events total: {total_alarm_events}")
    lines.append(f"Avg job duration  : {human_secs(avg_duration)}")
    lines.append("")
    lines.append("Jobs list:")

    if not jobs:
        lines.append("  - No jobs in this date range.")
    else:
        for j in jobs:
            jid = int(j["id"])
            dur = per_job_duration.get(jid, 0.0)
            lines.append(
                f"  - Job {jid}: {j.get('job_name')} | {j.get('status')} | Dur {human_secs(dur)} | Material {j.get('material') or '-'}"
            )

    report = "\n".join(lines)
    print(report)

    if args.export:
        outdir = os.path.join(os.getcwd(), "exports")
        ensure_dir(outdir)

        if start_date == end_date:
            fname = f"daily_summary_{start_date.isoformat()}.txt"
        else:
            fname = f"daily_summary_{start_date.isoformat()}_to_{end_date.isoformat()}.txt"

        path = os.path.join(outdir, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(report)

        print("\n--- Export ---")
        print(f"Report: {path}")

    conn.close()
    print("\nDone ✅")


if __name__ == "__main__":
    main()
