# CNC IIoT Backend (GRBL Telemetry + Job Tracking)

A simulation-driven **Industrial IoT backend** for a CNC laser engraver running **GRBL**.  
This project ingests GRBL-like telemetry logs, decodes machine state, stores telemetry/events in **SQLite**, tracks the **job lifecycle**, and generates reports + CSV exports.

> Current status: Backend demo pipeline works (simulation).  
> Next milestones: README polish + screenshots → dashboard → real-time GRBL serial connection.

---

## What it does
- Ingests GRBL-style telemetry logs (simulation)
- Decodes status messages (state + position, etc.)
- Stores **telemetry** and **events** in SQLite
- Tracks job lifecycle: **created → started → finished**
- Auto-finalises jobs based on telemetry timestamps
- Generates a **job summary report**
- Exports CSV files for reporting

## What this project demonstrates

- Industrial telemetry ingestion and decoding (GRBL-style machine data)  
- State-based job lifecycle tracking (created → started → finished)  
- Database schema design, versioning, and migrations (SQLite)  
- OT/IT-style data pipelines for industrial equipment  
- Automated job summaries and CSV-based reporting  
- Clean separation between ingestion, storage, processing, and reporting logic

## Motivation

This project was built to develop practical experience in industrial automation and IIoT concepts, with a focus on bridging operational technology (OT) and information technology (IT). It serves as a hands-on learning platform to explore telemetry ingestion, machine state tracking, and data-driven monitoring for real-world industrial equipment.    

---

## Repo contents (main scripts)
- `demo_run.ps1` — one-command demo run
- `log_to_db.py` — telemetry ingestion + DB logging
- `read_grbl_log.py` — reads/parses GRBL log input
- `migrate_schema*.py` — DB schema setup/migrations
- `job_report.py` / `export_job_report.py` — reporting + CSV exports
- `dashboard/` and `dashboard_app.py` — dashboard work-in-progress

---

## Quick start (Windows)
### 1) Create a virtual environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1



