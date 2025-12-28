import sqlite3

conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

telemetry_count = cur.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0]
event_count = cur.execute("SELECT COUNT(*) FROM events").fetchone()[0]

print("Telemetry rows:", telemetry_count)
print("Event rows:", event_count)

conn.close()
