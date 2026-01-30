from pathlib import Path
from datetime import datetime, timezone
import sqlite3

LOG_PATH = Path("grbl_sample.log")
DB_PATH = Path("cnc_iiot.db")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_app_state(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()


def get_active_job_id(conn: sqlite3.Connection) -> int | None:
    ensure_app_state(conn)
    row = conn.execute("SELECT value FROM app_state WHERE key='active_job_id'").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except ValueError:
        return None


def init_db(conn: sqlite3.Connection) -> None:
    ensure_app_state(conn)


def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    message: str,
    raw: str,
    level: str = "info",
    category: str = "system",
    code: str | None = None,
    meta_json: str | None = None,
) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    ts_utc = now_utc_iso()
    job_id = get_active_job_id(conn)
    raw = raw if raw is not None else ""

    conn.execute(
        """
        INSERT INTO events (ts, event_type, message, raw, ts_utc, level, category, code, job_id, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ts, event_type, message, raw, ts_utc, level, category, code, job_id, meta_json),
    )


def log_telemetry(
    conn: sqlite3.Connection,
    state: str,
    x: float,
    y: float,
    z: float,
    feed: int,
    spindle: int,
    raw: str,
) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    ts_utc = now_utc_iso()
    job_id = get_active_job_id(conn)

    conn.execute(
        """
        INSERT INTO telemetry (
            ts, state, x, y, z, feed, spindle, raw,
            ts_utc, source,
            mpos_x, mpos_y, mpos_z,
            job_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts, state, x, y, z, int(feed), int(spindle), raw,
            ts_utc, "grbl",
            x, y, z,
            job_id
        ),
    )


def finalize_job_from_telemetry(conn: sqlite3.Connection, job_id: int) -> None:
    row = conn.execute("""
        SELECT MIN(ts_utc), MAX(ts_utc), COUNT(*)
        FROM telemetry
        WHERE job_id=?
    """, (job_id,)).fetchone()

    if not row:
        return

    t_start, t_end, n = row
    if not t_start or not t_end or n == 0:
        return

    conn.execute("""
        UPDATE jobs
        SET
            started_ts_utc = COALESCE(started_ts_utc, ?),
            finished_ts_utc = ?,
            status = CASE
                WHEN status IN ('created','running','paused') THEN 'finished'
                ELSE status
            END
        WHERE id=?
    """, (t_start, t_end, job_id))

    ts_local = datetime.now().isoformat(timespec="seconds")
    ts_utc = now_utc_iso()
    conn.execute("""
        INSERT INTO events (ts, ts_utc, event_type, message, raw, level, category, code, job_id, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ts_local, ts_utc,
        "job",
        f"Job auto-finalized from telemetry (samples={n})",
        "",
        "info", "job", "AUTO_FINALIZE", job_id, None
    ))


def parse_status(line: str):
    content = line.strip("<>")
    parts = content.split("|")
    state = parts[0]

    pos_part = next(p for p in parts if p.startswith("MPos:"))
    fs_part = next(p for p in parts if p.startswith("FS:"))

    x, y, z = map(float, pos_part.replace("MPos:", "").split(","))
    feed, spindle = map(int, fs_part.replace("FS:", "").split(","))

    return state, x, y, z, feed, spindle


# ✅ THIS IS THE IMPORTANT NEW FUNCTION
def process_grbl_line(conn: sqlite3.Connection, line: str) -> None:
    line = line.strip()
    if not line:
        return

    if line.lower().startswith("grbl"):
        log_event(conn, "startup", "GRBL startup banner", line, category="system")

    elif line == "ok":
        log_event(conn, "ok", "Command acknowledged", line, category="system")

    elif line.startswith("ALARM:"):
        log_event(conn, "alarm", line, line, level="error", category="grbl", code="ALARM")

    elif line.startswith("<") and line.endswith(">"):
        state, x, y, z, feed, spindle = parse_status(line)
        log_telemetry(conn, state, x, y, z, feed, spindle, line)

    else:
        log_event(conn, "raw", "Unclassified line", line, category="system")


def main() -> None:
    if not LOG_PATH.exists():
        print("Could not find grbl_sample.log in this folder.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        active_job_id = get_active_job_id(conn)
        print("Active job_id:", active_job_id)

        for raw in LOG_PATH.read_text(encoding="utf-8").splitlines():
            process_grbl_line(conn, raw)

        if active_job_id is not None:
            finalize_job_from_telemetry(conn, active_job_id)

        conn.commit()
        print("Done Logged to database:", DB_PATH.resolve())

    finally:
        conn.close()


if __name__ == "__main__":
    main()

