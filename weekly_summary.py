# weekly_summary.py
# CNC IIoT - Weekly Summary Export (v1.0)
#
# Produces a per-job summary table for the last N days (default 7).
# Exports CSV + JSON to ./exports
#
# Usage:
#   python weekly_summary.py --export
#   python weekly_summary.py --days 7 --export
#   python weekly_summary.py --date 2025-12-27 --export   # one day
#   python weekly_summary.py --days 30 --export --db "C:\path\to\cnc_iiot.db"

import argparse
import csv
import json
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


def export_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


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


def telemetry_span_seconds(conn: sqlite3.Connection, job_id: int, sample_interval_s_default: float = 1.0) -> float:
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


def main():
    parser = argparse.ArgumentParser(description="CNC IIoT Weekly Summary Export (UTC)")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB (default: cnc_iiot.db)")
    parser.add_argument("--date", help="UTC date in YYYY-MM-DD (overrides --days and produces 1-day export)")
    parser.add_argument("--days", type=int, default=7, help="Number of days ending today UTC (default: 7)")
    parser.add_argument("--export", action="store_true", help="Export CSV + JSON to ./exports")
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

    rows_out: List[Dict[str, Any]] = []
    for j in jobs:
        jid = int(j["id"])

        dur = compute_duration(j.get("started_ts_utc"), j.get("finished_ts_utc"))
        if dur == 0.0:
            dur = telemetry_span_seconds(conn, jid, sample_interval_s_default=1.0)

        alarms = count_alarm_events(conn, jid)

        rows_out.append({
            "job_id": jid,
            "job_name": j.get("job_name"),
            "status": j.get("status"),
            "material": j.get("material"),
            "created_ts_utc": j.get("created_ts_utc"),
            "started_ts_utc": j.get("started_ts_utc"),
            "finished_ts_utc": j.get("finished_ts_utc"),
            "duration_seconds": dur,
            "duration_human": human_secs(dur),
            "alarm_events": alarms,
        })

    # Print a small console summary
    print("\n" + "=" * 58)
    print(" CNC IIoT - WEEKLY SUMMARY (v1.0)")
    print("=" * 58)
    if start_date == end_date:
        print(f"Date (UTC): {start_date.isoformat()}")
    else:
        print(f"Range (UTC): {start_date.isoformat()} to {end_date.isoformat()}")
    print(f"Jobs: {len(rows_out)}")

    if rows_out:
        total_alarm = sum(r["alarm_events"] for r in rows_out)
        avg_dur = sum(r["duration_seconds"] for r in rows_out) / len(rows_out)
        print(f"Total alarms: {total_alarm}")
        print(f"Avg duration: {human_secs(avg_dur)}")
    else:
        print("No jobs in this range.")

    if args.export:
        outdir = os.path.join(os.getcwd(), "exports")
        ensure_dir(outdir)

        if start_date == end_date:
            base = f"weekly_summary_{start_date.isoformat()}"
        else:
            base = f"weekly_summary_{start_date.isoformat()}_to_{end_date.isoformat()}"

        csv_path = os.path.join(outdir, base + ".csv")
        json_path = os.path.join(outdir, base + ".json")

        export_csv(csv_path, rows_out)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(rows_out, f, indent=2)

        print("\n--- Export ---")
        print(f"CSV : {csv_path}")
        print(f"JSON: {json_path}")

    conn.close()
    print("\nDone ✅")


if __name__ == "__main__":
    main()
