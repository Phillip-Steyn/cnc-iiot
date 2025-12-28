\# CNC IIoT Project



This project is a simulation-based Industrial IoT backend for a CNC laser engraver using GRBL.



\## What it does

\- Reads GRBL-style telemetry logs

\- Decodes machine status

\- Stores telemetry and events in SQLite

\- Tracks job lifecycle (created → started → finished)

\- Auto-finalises jobs

\- Generates job summary reports

\- Exports CSV files



\## How to run

```powershell

.\\demo\_run.ps1



