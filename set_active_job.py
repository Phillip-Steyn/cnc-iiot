import sqlite3

DB = "cnc_iiot.db"

def set_active_job(job_id: int | None):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT)")
    if job_id is None:
        cur.execute("DELETE FROM app_state WHERE key='active_job_id'")
        print("Active job cleared.")
    else:
        cur.execute("INSERT OR REPLACE INTO app_state (key, value) VALUES ('active_job_id', ?)", (str(job_id),))
        print("Active job set to:", job_id)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Change this number when you want a different active job
    set_active_job(1)
