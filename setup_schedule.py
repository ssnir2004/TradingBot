"""Registers all 11 Windows Task Scheduler entries for the bot: log rotation,
keep-awake, 7x premarket prefilter scans, the 5-minute trading cycle, and
the end-of-day dashboard/summary. Windows only (uses schtasks/powercfg).
Safe to re-run — every task is created with /F (overwrite). Run
cleanup_schedule.py to tear everything down.
"""
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VENV_PY = (Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe").resolve()
PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")


def _check_prereqs():
    for script in ("cycle.py", "morning_prefilter.py", "compute_perf.py", "rotate_logs.py"):
        if not (PROJECT_DIR / script).exists():
            sys.exit(f"STOP: missing {script} in {PROJECT_DIR}")
    if not VENV_PY.exists():
        sys.exit(f"STOP: venv python not found at {VENV_PY}. Create the venv first.")
    check = subprocess.run(["where", "schtasks"], capture_output=True, text=True)
    if check.returncode != 0:
        sys.exit("STOP: schtasks not found. This script only works on Windows.")


def _et_to_local(hh: int, mm: int) -> str:
    """Convert an America/New_York HH:MM to this machine's local time HH:MM."""
    today = datetime.now().date()
    et_dt = datetime(today.year, today.month, today.day, hh, mm, tzinfo=ET)
    local_dt = et_dt.astimezone()
    return local_dt.strftime("%H:%M")


def _detect_date_format() -> str:
    """Return today's date formatted to match this machine's locale, for
    schtasks' /SD parameter. Falls back to US format if detection fails."""
    try:
        result = subprocess.run(
            ["powershell", "-c", "(Get-Culture).DateTimeFormat.ShortDatePattern"],
            capture_output=True, text=True, timeout=10,
        )
        pattern = result.stdout.strip()
    except Exception:
        pattern = ""

    today = datetime.now()
    if pattern.lower() in ("yyyy/mm/dd", "yyyy-mm-dd"):
        return today.strftime("%Y/%m/%d")
    # default / fallback: US short date pattern M/d/yyyy
    return f"{today.month}/{today.day}/{today.year}"


def _create_task(name: str, tr: list[str], schedule_args: list[str]) -> bool:
    cmd = [
        "schtasks", "/create", "/tn", name,
        "/tr", " ".join(f'"{part}"' for part in tr),
        *schedule_args,
        "/F",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"FAILED to create {name}: {result.stderr.strip()}")
        return False
    print(f"Created {name}")
    return True


def main():
    _check_prereqs()

    weekdays = "MON,TUE,WED,THU,FRI"
    created = 0

    def weekly(name, script_args, et_hh, et_mm):
        nonlocal created
        local_time = _et_to_local(et_hh, et_mm)
        tr = [str(VENV_PY), *script_args] if isinstance(script_args, list) else [str(VENV_PY), script_args]
        if _create_task(name, tr, ["/sc", "WEEKLY", "/D", weekdays, "/ST", local_time]):
            created += 1

    # HT_LogRotate: 09:25 ET
    weekly("HT_LogRotate", str(PROJECT_DIR / "rotate_logs.py"), 9, 25)

    # HT_KeepAwake: 09:30 ET, raw command (no venv python)
    local_time = _et_to_local(9, 30)
    if _create_task("HT_KeepAwake", ["powercfg", "/change", "standby-timeout-ac", "0"],
                     ["/sc", "WEEKLY", "/D", weekdays, "/ST", local_time]):
        created += 1

    # HT_Prefilter_01..07: 09:55 -> 12:55 ET, every 30 min
    prefilter_times = [(9, 55), (10, 25), (10, 55), (11, 25), (11, 55), (12, 25), (12, 55)]
    for i, (hh, mm) in enumerate(prefilter_times, start=1):
        weekly(f"HT_Prefilter_{i:02d}", str(PROJECT_DIR / "morning_prefilter.py"), hh, mm)

    # HT_Cycle: every 5 minutes, starting 10:00 ET, daily
    start_date = _detect_date_format()
    local_start_time = _et_to_local(10, 0)
    tr = [str(VENV_PY), str(PROJECT_DIR / "cycle.py")]
    if _create_task("HT_Cycle", tr,
                     ["/sc", "MINUTE", "/MO", "5", "/SD", start_date, "/ST", local_start_time]):
        created += 1

    # HT_Dashboard: 16:05 ET
    weekly("HT_Dashboard", str(PROJECT_DIR / "compute_perf.py"), 16, 5)

    query = subprocess.run(["schtasks", "/query", "/tn", "HT_*", "/fo", "TABLE"],
                            capture_output=True, text=True)
    print(query.stdout)

    print(f"SCHEDULED: {created} tasks created. "
          "The bot will fire on its own starting next market open. Watch Telegram.")


if __name__ == "__main__":
    main()
