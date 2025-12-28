import sqlite3

conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

print("Jobs rows:", cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0])

print("\nLatest jobs:")
for r in cur.execute("""
    SELECT id, job_name, status, created_ts_utc, started_ts_utc, finished_ts_utc
    FROM jobs ORDER BY id DESC LIMIT 5
"""):
    print(r)

print("\nLatest job events:")
for r in cur.execute("""
    SELECT id, ts_utc, level, category, message, job_id, raw
    FROM events
    WHERE category='job'
    ORDER BY id DESC LIMIT 20
"""):
    print(r)

conn.close()
