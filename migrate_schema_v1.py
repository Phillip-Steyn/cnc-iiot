import sqlite3

DB = "cnc_iiot.db"

def col_exists(cur, table, col):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    return col in cols

def add_col(cur, table, col_def):
    # col_def example: "ts_utc TEXT"
    col_name = col_def.split()[0]
    if not col_exists(cur, table, col_name):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

def index_exists(cur, index_name):
    row = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,)
    ).fetchone()
    return row is not None

def create_index(cur, index_sql, index_name):
    if not index_exists(cur, index_name):
        cur.execute(index_sql)

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Good defaults for an IIoT logger
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # 1) Telemetry: add new columns
    add_col(cur, "telemetry", "ts_utc TEXT")
    add_col(cur, "telemetry", "source TEXT DEFAULT 'grbl'")
    add_col(cur, "telemetry", "mpos_x REAL")
    add_col(cur, "telemetry", "mpos_y REAL")
    add_col(cur, "telemetry", "mpos_z REAL")
    add_col(cur, "telemetry", "wpos_x REAL")
    add_col(cur, "telemetry", "wpos_y REAL")
    add_col(cur, "telemetry", "wpos_z REAL")
    add_col(cur, "telemetry", "line INTEGER")
    add_col(cur, "telemetry", "job_id INTEGER")

    # Map your existing x,y,z into mpos_x,y,z (keeps old columns too)
    cur.execute("""
        UPDATE telemetry
        SET
            ts_utc = COALESCE(ts_utc, ts),
            mpos_x = COALESCE(mpos_x, x),
            mpos_y = COALESCE(mpos_y, y),
            mpos_z = COALESCE(mpos_z, z)
    """)

    # 2) Events: add new columns
    add_col(cur, "events", "ts_utc TEXT")
    add_col(cur, "events", "level TEXT DEFAULT 'info'")
    add_col(cur, "events", "category TEXT DEFAULT 'system'")
    add_col(cur, "events", "code TEXT")
    add_col(cur, "events", "meta_json TEXT")
    add_col(cur, "events", "job_id INTEGER")

    # Map old fields:
    # - ts -> ts_utc
    # - event_type -> category (good enough for now)
    cur.execute("""
        UPDATE events
        SET
            ts_utc = COALESCE(ts_utc, ts),
            category = COALESCE(category, event_type)
    """)

    # 3) Create jobs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            created_ts_utc TEXT NOT NULL,
            started_ts_utc TEXT,
            finished_ts_utc TEXT,
            status TEXT NOT NULL DEFAULT 'created',
            material TEXT,
            notes TEXT
        )
    """)

    # 4) Indexes (safe)
    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_telemetry_ts_utc ON telemetry(ts_utc)", "idx_telemetry_ts_utc")
    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_telemetry_state ON telemetry(state)", "idx_telemetry_state")
    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_telemetry_job ON telemetry(job_id)", "idx_telemetry_job")

    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_events_ts_utc ON events(ts_utc)", "idx_events_ts_utc")
    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_events_category ON events(category)", "idx_events_category")
    create_index(cur, "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id)", "idx_events_job")

    conn.commit()
    conn.close()
    print("✅ Migration to v1 done (no data deleted).")

if __name__ == "__main__":
    main()
