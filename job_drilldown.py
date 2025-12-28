# job_drilldown.py
# CNC IIoT - Job Drill-Down (v1.3) + Export + KPI v2 + Efficiency Score
# Usage:
#   python job_drilldown.py 1
#   python job_drilldown.py --latest
#   python job_drilldown.py 1 --export
#   python job_drilldown.py 1 --export --db "C:\path\to\cnc_iiot.db"

import argparse
import csv
import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any


DB_PATH_DEFAULT = "cnc_iiot.db"


def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s2 = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s2)
    except ValueError:
        return None


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def pick_first(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def fetchone_dict(cur: sqlite3.Cursor, query: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    cur.execute(query, params)
    row = cur.fetchone()
    if not row:
        return None
    return {desc[0]: row[i] for i, desc in enumerate(cur.description)}


def fetchall_dicts(cur: sqlite3.Cursor, query: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    cur.execute(query, params)
    rows = cur.fetchall()
    if not rows:
        return []
    colnames = [d[0] for d in cur.description]
    return [{colnames[i]: r[i] for i in range(len(colnames))} for r in rows]


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


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def export_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def drilldown(db_path: str, job_id: Optional[int] = None, latest: bool = False, export: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Resolve job_id
    if latest and job_id is None:
        row = cur.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            print("No jobs found.")
            return
        job_id = int(row[0])

    if job_id is None:
        print("Please provide a job_id or use --latest.")
        return

    # ---- JOB META ----
    job = fetchone_dict(
        cur,
        """
        SELECT id, job_name, status, material, notes,
               created_ts_utc, started_ts_utc, finished_ts_utc
        FROM jobs
        WHERE id = ?
        """,
        (job_id,),
    )
    if not job:
        print(f"Job {job_id} not found.")
        return

    created = job.get("created_ts_utc")
    started = job.get("started_ts_utc")
    finished = job.get("finished_ts_utc")
    duration_total = compute_duration(started, finished)

    print("\n" + "=" * 58)
    print(" CNC IIoT - JOB DRILL-DOWN REPORT (v1.3)")
    print("=" * 58)

    print(f"Job ID   : {job.get('id')}")
    print(f"Name     : {job.get('job_name')}")
    print(f"Status   : {job.get('status')}")
    print(f"Material : {job.get('material') or '-'}")
    print(f"Notes    : {job.get('notes') or '-'}")
    print(f"Created  : {created or '-'}")
    print(f"Started  : {started or '-'}")
    print(f"Finished : {finished or '-'}")

    # ---- TELEMETRY ----
    tel_cols = get_table_columns(conn, "telemetry")
    ts_col = pick_first(tel_cols, ["ts_utc", "timestamp_utc", "timestamp", "ts"])
    job_col = pick_first(tel_cols, ["job_id"])
    state_col = pick_first(tel_cols, ["state", "machine_state", "status"])
    feed_col = pick_first(tel_cols, ["feed", "feed_rate", "f"])
    power_col = pick_first(tel_cols, [
        "laser_power", "power", "s", "s_value", "spindle", "spindle_speed", "rpm", "pwm"
    ])

    telemetry_rows: List[Dict[str, Any]] = []
    if ts_col and job_col:
        q = f"""
            SELECT *
            FROM telemetry
            WHERE {job_col} = ?
            ORDER BY {ts_col} ASC
        """
        telemetry_rows = fetchall_dicts(cur, q, (job_id,))

    print("\n--- Telemetry ---")

    # Initialize KPI fields for export
    tel_span = 0.0
    feed_avg = feed_max = None
    power_avg = power_max = None

    active = idle = alarm = None
    active_pct = idle_pct = alarm_pct = None

    eff_score = None  # 0..100

    if not telemetry_rows:
        print("No telemetry samples for this job.")
    else:
        first_ts = telemetry_rows[0].get(ts_col)
        last_ts = telemetry_rows[-1].get(ts_col)
        samples = len(telemetry_rows)

        tel_span = compute_duration(first_ts, last_ts)

        print(f"Samples   : {samples}")
        print(f"First ts  : {first_ts}")
        print(f"Last ts   : {last_ts}")

        # Fallback if timestamps are identical (common in sim logs)
        if tel_span == 0.0 and samples > 1:
            sample_interval_s = 1.0
            tel_span = (samples - 1) * sample_interval_s
            print(f"Span      : {human_secs(tel_span)} (estimated @ {sample_interval_s:.1f}s/sample)")
        else:
            print(f"Span      : {human_secs(tel_span)}")

        def extract_numeric(colname: Optional[str]) -> List[float]:
            if not colname:
                return []
            vals = []
            for r in telemetry_rows:
                v = r.get(colname)
                try:
                    if v is None:
                        continue
                    vals.append(float(v))
                except (ValueError, TypeError):
                    continue
            return vals

        feed_vals = extract_numeric(feed_col)
        power_vals = extract_numeric(power_col)

        if feed_col and feed_vals:
            feed_avg = sum(feed_vals) / len(feed_vals)
            feed_max = max(feed_vals)
            print(f"Feed avg  : {feed_avg:.2f}")
            print(f"Feed max  : {feed_max:.2f}")
        else:
            print("Feed      : (no feed column or numeric values)")

        if power_col and power_vals:
            power_avg = sum(power_vals) / len(power_vals)
            power_max = max(power_vals)
            print(f"Power avg : {power_avg:.2f}")
            print(f"Power max : {power_max:.2f}")
        else:
            if power_col:
                print(f"Power     : (column '{power_col}' found, but values not numeric)")
            else:
                print("Power     : (no power column detected)")

        # State breakdown + KPI v2
        if state_col:
            counts: Dict[str, int] = {}
            for r in telemetry_rows:
                s = r.get(state_col)
                s = str(s) if s is not None else "UNKNOWN"
                counts[s] = counts.get(s, 0) + 1

            print("\nState counts:")
            for k, v in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
                print(f"  - {k}: {v}")

            active = 0
            idle = 0
            alarm = 0

            for r in telemetry_rows:
                s = str(r.get(state_col) or "").lower()
                if "alarm" in s:
                    alarm += 1
                elif "idle" in s:
                    idle += 1
                else:
                    active += 1

            total = active + idle + alarm
            active_pct = 100.0 * safe_div(active, total)
            idle_pct = 100.0 * safe_div(idle, total)
            alarm_pct = 100.0 * safe_div(alarm, total)

            print("\nKPI (v2):")
            print(f"  Active samples : {active} ({active_pct:.1f}%)")
            print(f"  Idle samples   : {idle} ({idle_pct:.1f}%)")
            print(f"  Alarm samples  : {alarm} ({alarm_pct:.1f}%)")

            # Efficiency Score v2 (simple + explainable)
            # Start at 100, penalize idle time and alarms (alarms harsher)
            eff_score = 100.0
            eff_score -= 0.6 * idle_pct
            eff_score -= 1.5 * alarm_pct
            eff_score = max(0.0, min(100.0, eff_score))

            print(f"  Efficiency score: {eff_score:.1f} / 100")
        else:
            print("\nKPI (v2): (state not available)")

    # Duration fallback: use telemetry span if job timestamps show 0s
    if duration_total == 0.0 and tel_span > 0:
        duration_total = tel_span

    print(f"Duration : {human_secs(duration_total)}")

    # ---- EVENTS ----
    evt_cols = get_table_columns(conn, "events")
    evt_job_col = pick_first(evt_cols, ["job_id"])
    evt_ts_col = pick_first(evt_cols, ["ts_utc", "timestamp_utc", "timestamp", "ts"])
    evt_type_col = pick_first(evt_cols, ["event_type", "type", "level", "category"])
    evt_msg_col = pick_first(evt_cols, ["message", "msg", "detail", "details", "text"])

    print("\n--- Events ---")
    events: List[Dict[str, Any]] = []
    if evt_job_col and evt_ts_col:
        q = f"""
            SELECT *
            FROM events
            WHERE {evt_job_col} = ?
            ORDER BY {evt_ts_col} ASC
        """
        events = fetchall_dicts(cur, q, (job_id,))
        if not events:
            print("No events for this job.")
        else:
            print(f"Events: {len(events)}")
            for e in events[:50]:
                ts = e.get(evt_ts_col)
                et = e.get(evt_type_col) if evt_type_col else None
                msg = e.get(evt_msg_col) if evt_msg_col else None
                et_s = str(et) if et is not None else "EVENT"
                msg_s = str(msg) if msg is not None else ""
                print(f"  [{ts}] {et_s}: {msg_s}".rstrip())
            if len(events) > 50:
                print(f"  ... ({len(events) - 50} more)")
    else:
        print("Events table exists, but required columns not found (need job_id + timestamp).")

    # Alarm events + alarm rate per minute (based on events)
    alarm_events = 0
    if events:
        for e in events:
            et = str(e.get(evt_type_col) or "").lower() if evt_type_col else ""
            msg = str(e.get(evt_msg_col) or "").lower() if evt_msg_col else ""
            if "alarm" in et or "alarm" in msg:
                alarm_events += 1

    duration_min = duration_total / 60.0 if duration_total else 0.0
    alarm_rate_per_min = (alarm_events / duration_min) if duration_min else 0.0

    print(f"\nAlarm events: {alarm_events}")
    print(f"Alarm rate : {alarm_rate_per_min:.2f} per minute")

    # ---- EXPORT ----
    if export:
        outdir = os.path.join(os.getcwd(), "exports")
        ensure_dir(outdir)

        tel_path = os.path.join(outdir, f"job_{job_id}_telemetry.csv")
        evt_path = os.path.join(outdir, f"job_{job_id}_events.csv")
        kpi_path = os.path.join(outdir, f"job_{job_id}_kpis.json")

        export_csv(tel_path, telemetry_rows)
        export_csv(evt_path, events)

        kpis = {
            "job_id": job_id,
            "job_name": job.get("job_name"),
            "status": job.get("status"),
            "material": job.get("material"),
            "notes": job.get("notes"),
            "created_ts_utc": created,
            "started_ts_utc": started,
            "finished_ts_utc": finished,

            "duration_seconds": duration_total,
            "duration_human": human_secs(duration_total),

            "telemetry_samples": len(telemetry_rows),
            "telemetry_span_seconds": tel_span,
            "telemetry_span_human": human_secs(tel_span),

            "feed_avg": feed_avg,
            "feed_max": feed_max,
            "power_avg": power_avg,
            "power_max": power_max,

            "active_samples": active,
            "idle_samples": idle,
            "alarm_samples": alarm,

            "active_pct": active_pct,
            "idle_pct": idle_pct,
            "alarm_pct": alarm_pct,

            "efficiency_score_v2": eff_score,

            "alarm_events": alarm_events,
            "alarm_rate_per_min": alarm_rate_per_min,

            "telemetry_cols_detected": {
                "ts_col": ts_col,
                "job_col": job_col,
                "state_col": state_col,
                "feed_col": feed_col,
                "power_col": power_col,
            },
        }

        with open(kpi_path, "w", encoding="utf-8") as f:
            json.dump(kpis, f, indent=2)

        print("\n--- Export ---")
        print(f"Telemetry CSV : {tel_path}")
        print(f"Events CSV    : {evt_path}")
        print(f"KPIs JSON     : {kpi_path}")

    print("\nDone ✅")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="CNC IIoT Job Drill-Down Report")
    parser.add_argument("job_id", nargs="?", type=int, help="Job ID to drill down")
    parser.add_argument("--latest", action="store_true", help="Use latest job in DB")
    parser.add_argument("--export", action="store_true", help="Export per-job files to ./exports")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB (default: cnc_iiot.db)")
    args = parser.parse_args()

    drilldown(db_path=args.db, job_id=args.job_id, latest=args.latest, export=args.export)


if __name__ == "__main__":
    main()
