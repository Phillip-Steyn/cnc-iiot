# job_compare.py
# CNC IIoT - Jobs Summary / Compare (v1.0)
# Works with 1 job now, becomes a leaderboard when you have more jobs.
#
# Usage:
#   python job_compare.py
#   python job_compare.py --export
#   python job_compare.py --db "C:\path\to\cnc_iiot.db" --export

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


def fetchall_dicts(cur: sqlite3.Cursor, query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur.execute(query, params)
    rows = cur.fetchall()
    if not rows:
        return []
    colnames = [d[0] for d in cur.description]
    return [{colnames[i]: r[i] for i in range(len(colnames))} for r in rows]


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


def efficiency_score_v2(idle_pct: float, alarm_pct: float) -> float:
    # Matches your drilldown v1.3 logic
    score = 100.0
    score -= 0.6 * idle_pct
    score -= 1.5 * alarm_pct
    return max(0.0, min(100.0, score))


def compute_job_kpis(conn: sqlite3.Connection, job: Dict[str, Any], sample_interval_s_default: float = 1.0) -> Dict[str, Any]:
    cur = conn.cursor()

    job_id = int(job["id"])
    started = job.get("started_ts_utc")
    finished = job.get("finished_ts_utc")
    duration_total = compute_duration(started, finished)

    # Telemetry column detection
    tel_cols = get_table_columns(conn, "telemetry")
    ts_col = pick_first(tel_cols, ["ts_utc", "timestamp_utc", "timestamp", "ts"])
    job_col = pick_first(tel_cols, ["job_id"])
    state_col = pick_first(tel_cols, ["state", "machine_state", "status"])
    feed_col = pick_first(tel_cols, ["feed", "feed_rate", "f"])
    power_col = pick_first(tel_cols, ["laser_power", "power", "s", "s_value", "spindle", "spindle_speed", "rpm", "pwm"])

    telemetry_rows: List[Dict[str, Any]] = []
    tel_span = 0.0

    feed_avg = feed_max = None
    power_avg = power_max = None

    active = idle = alarm = 0
    active_pct = idle_pct = alarm_pct = 0.0
    eff_score = None

    if ts_col and job_col:
        telemetry_rows = fetchall_dicts(
            cur,
            f"SELECT * FROM telemetry WHERE {job_col} = ? ORDER BY {ts_col} ASC",
            (job_id,),
        )

    if telemetry_rows:
        first_ts = telemetry_rows[0].get(ts_col)
        last_ts = telemetry_rows[-1].get(ts_col)
        samples = len(telemetry_rows)

        tel_span = compute_duration(first_ts, last_ts)
        if tel_span == 0.0 and samples > 1:
            tel_span = (samples - 1) * sample_interval_s_default

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

        if feed_vals:
            feed_avg = sum(feed_vals) / len(feed_vals)
            feed_max = max(feed_vals)

        if power_vals:
            power_avg = sum(power_vals) / len(power_vals)
            power_max = max(power_vals)

        if state_col:
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

            eff_score = efficiency_score_v2(idle_pct, alarm_pct)

    # Duration fallback: use telemetry span if job timestamps show 0s
    if duration_total == 0.0 and tel_span > 0:
        duration_total = tel_span

    # Events alarm count + alarm rate per minute
    evt_cols = get_table_columns(conn, "events")
    evt_job_col = pick_first(evt_cols, ["job_id"])
    evt_ts_col = pick_first(evt_cols, ["ts_utc", "timestamp_utc", "timestamp", "ts"])
    evt_type_col = pick_first(evt_cols, ["event_type", "type", "level", "category"])
    evt_msg_col = pick_first(evt_cols, ["message", "msg", "detail", "details", "text"])

    alarm_events = 0
    event_count = 0

    if evt_job_col and evt_ts_col:
        events = fetchall_dicts(
            cur,
            f"SELECT * FROM events WHERE {evt_job_col} = ? ORDER BY {evt_ts_col} ASC",
            (job_id,),
        )
        event_count = len(events)
        for e in events:
            et = str(e.get(evt_type_col) or "").lower() if evt_type_col else ""
            msg = str(e.get(evt_msg_col) or "").lower() if evt_msg_col else ""
            if "alarm" in et or "alarm" in msg:
                alarm_events += 1

    duration_min = duration_total / 60.0 if duration_total else 0.0
    alarm_rate_per_min = (alarm_events / duration_min) if duration_min else 0.0

    return {
        "job_id": job_id,
        "job_name": job.get("job_name"),
        "status": job.get("status"),
        "material": job.get("material"),
        "created_ts_utc": job.get("created_ts_utc"),
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

        "active_samples": active if telemetry_rows else None,
        "idle_samples": idle if telemetry_rows else None,
        "alarm_samples": alarm if telemetry_rows else None,

        "active_pct": active_pct if telemetry_rows else None,
        "idle_pct": idle_pct if telemetry_rows else None,
        "alarm_pct": alarm_pct if telemetry_rows else None,

        "efficiency_score_v2": eff_score,

        "event_count": event_count,
        "alarm_events": alarm_events,
        "alarm_rate_per_min": alarm_rate_per_min,
    }


def print_table(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No jobs found.")
        return

    # simple fixed columns
    headers = [
        ("job_id", "ID"),
        ("job_name", "Name"),
        ("status", "Status"),
        ("duration_human", "Dur"),
        ("telemetry_samples", "Tel"),
        ("idle_pct", "Idle%"),
        ("alarm_pct", "Alarm%"),
        ("efficiency_score_v2", "Eff/100"),
        ("alarm_events", "AlarmEv"),
    ]

    # prepare printable values
    def fmt(v: Any) -> str:
        if v is None:
            return "-"
        if isinstance(v, float):
            return f"{v:.1f}"
        return str(v)

    # compute widths
    widths = []
    for key, title in headers:
        maxw = len(title)
        for r in rows:
            maxw = max(maxw, len(fmt(r.get(key))))
        widths.append(maxw)

    # print header
    line = " | ".join(title.ljust(widths[i]) for i, (_, title) in enumerate(headers))
    print("\n" + line)
    print("-" * len(line))

    # print rows sorted by ID
    for r in sorted(rows, key=lambda x: x["job_id"]):
        rowline = " | ".join(fmt(r.get(key)).ljust(widths[i]) for i, (key, _) in enumerate(headers))
        print(rowline)

    # quick insights
    print("\nInsights:")
    if len(rows) == 1:
        r = rows[0]
        print(f"- Only 1 job in DB right now (Job {r['job_id']}). Add more jobs to see comparisons.")
    else:
        ranked = sorted([r for r in rows if r.get("efficiency_score_v2") is not None],
                        key=lambda x: x["efficiency_score_v2"], reverse=True)
        if ranked:
            best = ranked[0]
            worst = ranked[-1]
            print(f"- Best efficiency: Job {best['job_id']} ({best['efficiency_score_v2']:.1f}/100)")
            print(f"- Worst efficiency: Job {worst['job_id']} ({worst['efficiency_score_v2']:.1f}/100)")


def main():
    parser = argparse.ArgumentParser(description="CNC IIoT Jobs Summary / Compare")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB (default: cnc_iiot.db)")
    parser.add_argument("--export", action="store_true", help="Export summary files to ./exports")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    jobs = fetchall_dicts(
        cur,
        """
        SELECT id, job_name, status, material, notes,
               created_ts_utc, started_ts_utc, finished_ts_utc
        FROM jobs
        ORDER BY id ASC
        """
    )

    summary_rows: List[Dict[str, Any]] = []
    for j in jobs:
        summary_rows.append(compute_job_kpis(conn, j))

    print_table(summary_rows)

    if args.export:
        outdir = os.path.join(os.getcwd(), "exports")
        ensure_dir(outdir)

        csv_path = os.path.join(outdir, "jobs_summary.csv")
        json_path = os.path.join(outdir, "jobs_summary.json")

        export_csv(csv_path, summary_rows)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary_rows, f, indent=2)

        print("\n--- Export ---")
        print(f"CSV  : {csv_path}")
        print(f"JSON : {json_path}")

    conn.close()
    print("\nDone ✅")


if __name__ == "__main__":
    main()
