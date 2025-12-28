$py = "C:/Users/phill/AppData/Local/Python/pythoncore-3.14-64/python.exe"

Write-Host "=== CNC IIoT DEMO RUN ==="

& $py reset_run.py
& $py reset_job_times.py
& $py set_active_job.py
& $py log_to_db.py
& $py job_report.py
& $py export_job_report.py

Write-Host "✅ Demo run complete. Check the /reports folder."
