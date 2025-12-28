import sqlite3

conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

cur.execute("DELETE FROM telemetry")
cur.execute("DELETE FROM events")
conn.commit()

print("✅ Cleared telemetry + events. Jobs kept.")
conn.close()
