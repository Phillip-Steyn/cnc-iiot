import sqlite3
from datetime import datetime

DB = "cnc_iiot.db"
JOB_ID = 1  # change this when you have more jobs


def parse_iso(ts: str):
    # handles "2025-12-27T18:33:31+00:00" and also legacy "2025-12-27T19:23:08"
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


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
        conn.close()
        return

    (job_id, job_name, status, created_ts, started_ts, finished_ts, material, notes) = job

    print("\n==============================")
    print(" CNC IIoT - JOB SUMMARY REPORT")
    print("==============================")
    print(f"Job ID   : {job_id}")
    print(f"Name     : {job_name}")
    print(f"Status   : {status}")
    print(f"Material : {material}")
    print(f"Notes    : {notes}")
    print(f"Created  : {created_ts}")
    print(f"Started  : {started_ts}")
    print(f"Finished : {finished_ts}")

    # Telemetry summary
    tel_count = cur.execute("SELECT COUNT(*) FROM telemetry WHERE job_id=?", (JOB_ID,)).fetchone()[0]
    print("\n--- Telemetry ---")
    print("Samples:", tel_count)

    if tel_count > 0:
        tel_minmax = cur.execute("""
            SELECT MIN(ts_utc), MAX(ts_utc)
            FROM telemetry
            WHERE job_id=?
        """, (JOB_ID,)).fetchone()
        t0, t1 = tel_minmax[0], tel_minmax[1]
        print("First ts_utc:", t0)
        print("Last  ts_utc:", t1)

        # Count by state
        print("\nSamples by state:")
        for r in cur.execute("""
            SELECT state, COUNT(*) as c
            FROM telemetry
            WHERE job_id=?
            GROUP BY state
            ORDER BY c DESC
        """, (JOB_ID,)):
            print(" ", r[0], "=", r[1])

        # Basic travel distance estimate from mpos
        rows = cur.execute("""
            SELECT mpos_x, mpos_y, mpos_z
            FROM telemetry
            WHERE job_id=?
            ORDER BY id ASC
        """, (JOB_ID,)).fetchall()

        dist = 0.0
        for i in range(1, len(rows)):
            x0, y0, z0 = rows[i - 1]
            x1, y1, z1 = rows[i]
            if None in (x0, y0, z0, x1, y1, z1):
                continue
            dx = (x1 - x0)
            dy = (y1 - y0)
            dz = (z1 - z0)
            dist += (dx*dx + dy*dy + dz*dz) ** 0.5

        print(f"\nEstimated travel distance (mm): {dist:.3f}")

        # Latest snapshot
        latest = cur.execute("""
            SELECT ts_utc, state, mpos_x, mpos_y, mpos_z, feed, spindle
            FROM telemetry
            WHERE job_id=?
            ORDER BY id DESC LIMIT 1
        """, (JOB_ID,)).fetchone()

        if latest:
            print("\nLatest telemetry:")
            print(" ts_utc :", latest[0])
            print(" state  :", latest[1])
            print(" mpos   :", (latest[2], latest[3], latest[4]))
            print(" feed   :", latest[5])
            print(" spindle:", latest[6])

    # Events summary
    ev_count = cur.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (JOB_ID,)).fetchone()[0]
    print("\n--- Events ---")
    print("Events:", ev_count)

    alarms = cur.execute("""
        SELECT COUNT(*)
        FROM events
        WHERE job_id=? AND (category='grbl' OR event_type='alarm' OR message LIKE 'ALARM:%')
    """, (JOB_ID,)).fetchone()[0]
    print("Alarm-related events:", alarms)

    print("\nLatest events:")
    for r in cur.execute("""
        SELECT ts_utc, level, category, message
        FROM events
        WHERE job_id=?
        ORDER BY id DESC LIMIT 10
    """, (JOB_ID,)):
        print(" ", r)

    conn.close()
    print("\n✅ Report complete.\n")


if __name__ == "__main__":
    main()
