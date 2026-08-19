"""Log rotation: archives yesterday-and-older log files, trims trades.csv to
the last 90 days, and rolls over safety-check-log.json past 5 MB. Safe to
run at any time of day; always exits 0. Run daily at 09:25 ET via
Task Scheduler, before the trading day starts.
"""
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_DIR = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_DIR / "logs"
ARCHIVE_DIR = LOGS_DIR / "archive"
TRADES_CSV = PROJECT_DIR / "trades.csv"
SAFETY_LOG_PATH = PROJECT_DIR / "safety-check-log.json"
TRADES_RETENTION_DAYS = 90
SAFETY_LOG_MAX_BYTES = 5 * 1024 * 1024

ET = ZoneInfo("America/New_York")


def _archive_dated(date_str: str) -> Path:
    target = ARCHIVE_DIR / date_str
    target.mkdir(parents=True, exist_ok=True)
    return target


def rotate_log_files() -> int:
    if not LOGS_DIR.exists():
        return 0

    today = datetime.now(ET).date()
    rotated = 0

    for path in LOGS_DIR.iterdir():
        if not path.is_file():
            continue
        if path.suffix not in (".log", ".jsonl"):
            continue

        mtime_et = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
        if mtime_et.date() >= today:
            continue

        target_dir = _archive_dated(mtime_et.strftime("%Y-%m-%d"))
        target_path = target_dir / path.name
        os.replace(path, target_path)
        rotated += 1

    return rotated


def rotate_trades_csv() -> int:
    if not TRADES_CSV.exists():
        return 0

    cutoff = datetime.now(ET) - timedelta(days=TRADES_RETENTION_DAYS)
    with open(TRADES_CSV, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = list(reader)

    if header is None:
        return 0

    keep, old = [], []
    for row in rows:
        try:
            ts = datetime.fromisoformat(row[0])
        except (ValueError, IndexError):
            keep.append(row)
            continue
        (keep if ts >= cutoff else old).append(row)

    if not old:
        return 0

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_name = f"trades_{datetime.now(ET).strftime('%Y%m%d')}.csv"
    with open(ARCHIVE_DIR / archive_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(old)

    with open(TRADES_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(keep)

    return 1


def rotate_safety_log() -> int:
    if not SAFETY_LOG_PATH.exists():
        return 0
    if SAFETY_LOG_PATH.stat().st_size <= SAFETY_LOG_MAX_BYTES:
        return 0

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_name = f"{datetime.now(ET).strftime('%Y%m%d')}-safety-check-log.json"
    os.replace(SAFETY_LOG_PATH, ARCHIVE_DIR / archive_name)
    SAFETY_LOG_PATH.touch()
    return 1


def main():
    rotated = rotate_log_files() + rotate_trades_csv() + rotate_safety_log()
    print(f"Rotated {rotated} files to logs/archive/.")


if __name__ == "__main__":
    main()
