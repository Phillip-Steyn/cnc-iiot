import sqlite3

conn = sqlite3.connect("cnc_iiot.db")
cur = conn.cursor()

print("\n--- TELEMETRY COLUMNS ---")
for row in cur.execute("PRAGMA table_info(telemetry)"):
    print(row)

print("\n--- EVENTS COLUMNS ---")
for row in cur.execute("PRAGMA table_info(events)"):
    print(row)

print("\n--- TABLES ---")
for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'"):
    print(row[0])

conn.close()
