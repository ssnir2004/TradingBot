"""Tears down every HT_* Windows Task Scheduler entry created by
setup_schedule.py and restores default power settings. Idempotent: running
it with nothing scheduled just prints "Nothing to clean."
"""
import csv
import io
import subprocess


def _list_ht_tasks() -> list[str]:
    result = subprocess.run(["schtasks", "/query", "/fo", "CSV", "/nh"],
                             capture_output=True, text=True)
    if result.returncode != 0:
        return []

    names = []
    reader = csv.reader(io.StringIO(result.stdout))
    for row in reader:
        if not row:
            continue
        task_name = row[0].strip().lstrip("\\")
        if task_name.startswith("HT_"):
            names.append(task_name)
    return names


def main():
    tasks = _list_ht_tasks()
    if not tasks:
        print("Nothing to clean.")
        return

    deleted = 0
    for name in tasks:
        result = subprocess.run(["schtasks", "/delete", "/tn", name, "/f"],
                                 capture_output=True, text=True)
        if result.returncode == 0:
            deleted += 1
        else:
            print(f"FAILED to delete {name}: {result.stderr.strip()}")

    subprocess.run(["powercfg", "/change", "standby-timeout-ac", "30"],
                    capture_output=True, text=True)

    print(f"CLEANUP DONE: {deleted} tasks deleted; default power settings restored.")


if __name__ == "__main__":
    main()
