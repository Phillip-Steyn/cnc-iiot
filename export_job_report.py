import csv
import sqlite3
from pathlib import Path

DB = "cnc_iiot.db"
JOB_ID = 1

OUT_DIR = Path("reports")
OUT_DIR.mkdir(exist_ok=True)

summary_csv = OUT_DIR / f"job_{JOB_ID}_summary.csv"
events_csv = OUT_DIR / f"job_{JOB_ID}_events.csv"
telemetry_csv = OUT_DIR / f"job_{JOB_ID}_telemetry.csv"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    job = cur.execute("""
        SELECT id, job_name, status, created_ts_utc, started_ts_utc, finished_ts_utc, material, notes
        FROM jobs
        WHERE id=?
    """, (JOB_ID,)).fetchone()

    if not job:
        print("❌ No job found with id:", JOB_ID)
        return

    # --- Summary metrics ---
    tel_count = cur.execute("SELECT COUNT(*) FROM telemetry WHERE job_id=?", (JOB_ID,)).fetchone()[0]
    ev_count = cur.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (JOB_ID,)).fetchone()[0]
    alarms = cur.execute("""
        SELECT COUNT(*)
        FROM events
        WHERE job_id=? AND (category='grbl' OR event_type='alarm' OR message LIKE 'ALARM:%')
    """, (JOB_ID,)).fetchone()[0]

    t0, t1 = cur.execute("""
        SELECT MIN(ts_utc), MAX(ts_utc)
        FROM telemetry
        WHERE job_id=?
    """, (JOB_ID,)).fetchone()

    state_counts = cur.execute("""
        SELECT state, COUNT(*)
        FROM telemetry
        WHERE job_id=?
        GROUP BY state
        ORDER BY COUNT(*) DESC
    """, (JOB_ID,)).fetchall()

    # write summary CSV (key/value style)
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["key", "value"])

        (job_id, name, status, created, started, finished, material, notes) = job
        w.writerow(["job_id", job_id])
        w.writerow(["job_name", name])
        w.writerow(["status", status])
        w.writerow(["material", material])
        w.writerow(["notes", notes])
        w.writerow(["created_ts_utc", created])
        w.writerow(["started_ts_utc", started])
        w.writerow(["finished_ts_utc", finished])

        w.writerow(["telemetry_samples", tel_count])
        w.writerow(["events_count", ev_count])
        w.writerow(["alarm_events", alarms])
        w.writerow(["telemetry_first_ts_utc", t0])
        w.writerow(["telemetry_last_ts_utc", t1])

        for state, c in state_counts:
            w.writerow([f"state_count_{state}", c])

    # --- Events CSV ---
    with events_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "ts_utc", "level", "category", "code", "message", "job_id"])
        for r in cur.execute("""
            SELECT id, ts_utc, level, category, code, message, job_id
            FROM events
            WHERE job_id=?
            ORDER BY id ASC
        """, (JOB_ID,)):
            w.writerow(r)

    # --- Telemetry CSV (latest N, or all if you want) ---
    with telemetry_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "ts_utc", "state", "mpos_x", "mpos_y", "mpos_z", "feed", "spindle", "job_id"])
        for r in cur.execute("""
            SELECT id, ts_utc, state, mpos_x, mpos_y, mpos_z, feed, spindle, job_id
            FROM telemetry
            WHERE job_id=?
            ORDER BY id ASC
        """, (JOB_ID,)):
            w.writerow(r)

    conn.close()

    print("✅ Exported:")
    print(" -", summary_csv.resolve())
    print(" -", events_csv.resolve())
    print(" -", telemetry_csv.resolve())


if __name__ == "__main__":
    main()
