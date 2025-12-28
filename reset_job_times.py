import sqlite3

DB = "cnc_iiot.db"
JOB_ID = 1

conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute("""
    UPDATE jobs
    SET started_ts_utc=NULL, finished_ts_utc=NULL, status='created'
    WHERE id=?
""", (JOB_ID,))

conn.commit()
conn.close()
print("✅ Reset started/finished timestamps for job", JOB_ID)
