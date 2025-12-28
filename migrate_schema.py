import sqlite3
from datetime import datetime

DB = "cnc_iiot.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Telemetry: frequent samples (status stream)
CREATE TABLE IF NOT EXISTS telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,                 -- ISO8601 UTC
    source TEXT NOT NULL DEFAULT 'grbl',   -- e.g. grbl, sim, etc.
    state TEXT,                           -- Idle/Run/Hold/Alarm...
    mpos_x REAL, mpos_y REAL, mpos_z REAL,
    wpos_x REAL, wpos_y REAL, wpos_z REAL,
    feed REAL,
    spindle REAL,
    line INTEGER,                         -- optional if available
    raw TEXT                              -- raw status line
);

CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(ts_utc);
CREATE INDEX IF NOT EXISTS idx_telemetry_state ON telemetry(state);

-- Events: discrete things that happen (alarms, connect/disconnect, job start/stop)
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'info',   -- info/warn/error
    category TEXT NOT NULL,               -- connection/grbl/job/system
    code TEXT,                            -- ALARM:1, ERR:...
    message TEXT NOT NULL,
    meta_json TEXT                        -- optional JSON as string
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_utc);
CREATE INDEX IF NOT EXISTS idx_events_cat ON events(category);

-- Jobs: business layer (this is the big “portfolio” value)
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    created_ts_utc TEXT NOT NULL,
    started_ts_utc TEXT,
    finished_ts_utc TEXT,
    status TEXT NOT NULL DEFAULT 'created',   -- created/running/paused/finished/failed
    material TEXT,
    notes TEXT
);

-- Link events and telemetry to a job (optional, but powerful)
ALTER TABLE telemetry ADD COLUMN job_id INTEGER;
ALTER TABLE events ADD COLUMN job_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_telemetry_job ON telemetry(job_id);
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
"""

def safe_exec(cur, sql: str):
    # SQLite will throw if column already exists; ignore those cases
    try:
        cur.executescript(sql)
    except sqlite3.OperationalError as e:
        # common on ALTER TABLE ADD COLUMN duplicates
        if "duplicate column name" in str(e).lower():
            pass
        else:
            raise

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    safe_exec(cur, SCHEMA)
    conn.commit()

    # quick sanity print
    tables = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    print("Tables:", [t[0] for t in tables])

    conn.close()
    print("✅ Schema upgrade done.")

if __name__ == "__main__":
    main()
