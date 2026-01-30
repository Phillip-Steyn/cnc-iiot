# run_ingest.py
import argparse
from cnc_iiot.grbl_sources import file_source, serial_source

# IMPORTANT:
# This file assumes you already have a function that processes ONE GRBL line
# and logs it into the DB. We’ll import it from your existing code.
#
# You likely already have this inside log_to_db.py (or similar).
# We'll support BOTH possibilities below.

def get_line_processor():
    """
    Try to locate an existing function in your project that processes one GRBL line.
    Adjust the import if needed.
    """
    # Option A: log_to_db.py defines process_grbl_line(line: str) or handle_grbl_line(...)
    try:
        import log_to_db
        for name in ["process_grbl_line", "handle_grbl_line", "ingest_grbl_line"]:
            if hasattr(log_to_db, name):
                return getattr(log_to_db, name)
    except Exception:
        pass

    # Option B: read_grbl_log.py defines parse/normalize and log_to_db logs it
    # If you don't have a single-line processor, we’ll create one in the next step.
    return None


def main():
    ap = argparse.ArgumentParser(description="CNC IIoT ingest runner (file or serial).")
    ap.add_argument("--mode", choices=["file", "serial"], required=True)
    ap.add_argument("--file", dest="file_path", help="Path to GRBL log file (mode=file).")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between file lines (simulation).")
    ap.add_argument("--port", help="Serial COM port e.g. COM3 (mode=serial).")
    ap.add_argument("--baud", type=int, default=115200, help="Serial baud rate (mode=serial).")
    args = ap.parse_args()

    processor = get_line_processor()
    if processor is None:
        raise SystemExit(
            "Could not find a single-line processor function in log_to_db.py.\n"
            "Next step: we’ll add one (process_grbl_line) so this runner can call it."
        )

    if args.mode == "file":
        if not args.file_path:
            raise SystemExit("Provide --file when mode=file")
        src = file_source(args.file_path, sleep_s=args.sleep)
    else:
        if not args.port:
            raise SystemExit("Provide --port when mode=serial (e.g. COM3)")
        src = serial_source(args.port, baud=args.baud)

    for line in src:
        processor(line)


if __name__ == "__main__":
    main()
