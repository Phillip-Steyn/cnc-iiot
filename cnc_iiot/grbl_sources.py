# cnc_iiot/grbl_sources.py
from __future__ import annotations

from typing import Iterable, Iterator, Optional
import time


def file_source(path: str, *, sleep_s: float = 0.0) -> Iterator[str]:
    """
    Yield GRBL lines from a text file (simulation).
    sleep_s can simulate time between lines.
    """
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            yield line
            if sleep_s > 0:
                time.sleep(sleep_s)


def serial_source(port: str, *, baud: int = 115200, timeout: float = 1.0) -> Iterator[str]:
    """
    Yield GRBL lines from a serial port (REAL CNC).
    Requires: pyserial
    """
    try:
        import serial  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pyserial is required for serial mode. Install with: python -m pip install pyserial"
        ) from e

    ser = serial.Serial(port=port, baudrate=baud, timeout=timeout)
    try:
        while True:
            raw = ser.readline()
            if not raw:
                continue
            line = raw.decode(errors="replace").strip()
            if line:
                yield line
    finally:
        try:
            ser.close()
        except Exception:
            pass
