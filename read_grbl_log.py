from pathlib import Path

LOG_PATH = Path("grbl_sample.log")

def main() -> None:
    if not LOG_PATH.exists():
        print("Could not find grbl_sample.log in this folder.")
        return

    print("Reading GRBL log...\n")

    for line in LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue

        # Simple "event decoding"
        if line.lower().startswith("grbl"):
            print(f"[STARTUP] {line}")

        elif line == "ok":
            print("[OK] Command acknowledged")

        elif line.startswith("ALARM:"):
            print(f"[ALARM] {line}")

        elif line.startswith("<") and line.endswith(">"):
            content = line.strip("<>")
            parts = content.split("|")

            state = parts[0]

            pos_part = next(p for p in parts if p.startswith("MPos:"))
            fs_part = next(p for p in parts if p.startswith("FS:"))

            x, y, z = map(float, pos_part.replace("MPos:", "").split(","))
            feed, spindle = map(int, fs_part.replace("FS:", "").split(","))

            print(
                f"[STATUS] state={state} "
                f"x={x:.3f} y={y:.3f} z={z:.3f} "
                f"feed={feed} spindle={spindle}"
            )

        else:
            print(f"[RAW] {line}")

    print("\nDone ✅")

if __name__ == "__main__":
    main()
