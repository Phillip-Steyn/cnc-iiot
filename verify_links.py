import sqlite3
conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

print("Telemetry with job_id=1:", cur.execute("SELECT COUNT(*) FROM telemetry WHERE job_id=1").fetchone()[0])
print("Events with job_id=1:", cur.execute("SELECT COUNT(*) FROM events WHERE job_id=1").fetchone()[0])

print("\nLatest telemetry rows (job_id, ts_utc, state, mpos_x):")
for r in cur.execute("SELECT job_id, ts_utc, state, mpos_x FROM telemetry ORDER BY id DESC LIMIT 5"):
    print(r)

print("\nLatest events rows (job_id, ts_utc, category, message):")
for r in cur.execute("SELECT job_id, ts_utc, category, message FROM events ORDER BY id DESC LIMIT 5"):
    print(r)

conn.close()
