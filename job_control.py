import sqlite3
from datetime import datetime, timezone

DB = "cnc_iiot.db"

def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def log_event(
    cur,
    level,
    category,
    message,
    code=None,
    job_id=None,
    meta_json=None,
    raw=None
):
    # events.raw is NOT NULL in your original schema
    raw = raw if raw is not None else ""

    cur.execute("""
        INSERT INTO events (
            ts,
            ts_utc,
            event_type,
            message,
            raw,
            level,
            category,
            code,
            job_id,
            meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now_utc(),          # ts
        now_utc(),          # ts_utc
        category,           # event_type (legacy)
        message,
        raw,
        level,
        category,
        code,
        job_id,
        meta_json
    ))

def create_job(job_name, material=None, notes=None):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO jobs (
            job_name,
            created_ts_utc,
            status,
            material,
            notes
        )
        VALUES (?, ?, 'created', ?, ?)
    """, (job_name, now_utc(), material, notes))

    job_id = cur.lastrowid

    log_event(
        cur,
        level="info",
        category="job",
        message=f"Job created: {job_name}",
        job_id=job_id
    )

    conn.commit()
    conn.close()
    return job_id

def start_job(job_id):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        UPDATE jobs
        SET started_ts_utc = ?, status = 'running'
        WHERE id = ? AND status IN ('created','paused')
    """, (now_utc(), job_id))

    log_event(
        cur,
        level="info",
        category="job",
        message="Job started",
        job_id=job_id
    )

    conn.commit()
    conn.close()

def stop_job(job_id, status="finished"):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        UPDATE jobs
        SET finished_ts_utc = ?, status = ?
        WHERE id = ? AND status IN ('running','paused')
    """, (now_utc(), status, job_id))

    log_event(
        cur,
        level="info",
        category="job",
        message=f"Job ended: {status}",
        job_id=job_id
    )

    conn.commit()
    conn.close()

# -------------------------------------------------
# Demo run (safe to run multiple times)
# -------------------------------------------------
if __name__ == "__main__":
    job_id = create_job(
        "Demo Engrave - Keychain",
        material="Birch plywood",
        notes="Sim run"
    )
    print("Created job:", job_id)

    start_job(job_id)
    print("Started job:", job_id)

    stop_job(job_id, "finished")
    print("Finished job:", job_id)
