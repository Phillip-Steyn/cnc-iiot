import sqlite3

conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

print("Telemetry rows:", cur.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0])
print("Events rows:", cur.execute("SELECT COUNT(*) FROM events").fetchone()[0])
print("Jobs rows:", cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0])

print("\nTelemetry ts -> ts_utc (first 3):")
for row in cur.execute("SELECT ts, ts_utc, x, mpos_x FROM telemetry LIMIT 3"):
    print(row)

print("\nEvents ts -> ts_utc (first 3):")
for row in cur.execute("SELECT ts, ts_utc, event_type, category FROM events LIMIT 3"):
    print(row)

conn.close()
