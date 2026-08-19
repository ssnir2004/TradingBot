"""The always-on trading engine process for the server deployment. Replaces
Windows Task Scheduler with an internal APScheduler running the same jobs
(premarket prefilter, the 5-minute trading cycle, the daily summary, and DB
maintenance) inside one long-lived process — this is what systemd's
trading-bot.service runs. The dashboard (run_dashboard.py) is a separate
process that only reads/writes the shared SQLite DB; it never talks to IBKR
directly, so the two processes never fight over an IBKR client id.
"""
import logging
import sys
import traceback
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import cycle
import morning_prefilter
import daily_summary
from src import db

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_service")


def _guarded(job_name: str, fn, *args, **kwargs):
    """Run a scheduled job, catching and logging any exception so one bad
    tick never kills the whole service process."""
    try:
        fn(*args, **kwargs)
    except Exception:
        logger.exception("Job %s failed", job_name)
        db.log_cycle_error(f"[{job_name}]\n{traceback.format_exc()}")


def job_prefilter():
    _guarded("prefilter", morning_prefilter.run_scan, morning_prefilter.DEFAULT_MIN_GAP_PCT,
              morning_prefilter.DEFAULT_MIN_PRICE, False)


def job_cycle():
    _guarded("cycle", cycle.run_cycle)


def job_emergency_check():
    _guarded("emergency_check", cycle.emergency_check)


def job_daily_summary():
    _guarded("daily_summary", daily_summary.run)


def job_maintenance():
    _guarded("maintenance", db.trim_old_rows)


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    logger.info("DB ready at %s", db.DB_PATH)

    scheduler = BlockingScheduler(timezone=ET)

    scheduler.add_job(
        job_prefilter, CronTrigger(
            day_of_week="mon-fri",
            hour="9-12", minute="25,55",
            timezone=ET,
        ),
        id="prefilter", misfire_grace_time=120,
    )

    scheduler.add_job(
        job_cycle, IntervalTrigger(minutes=5),
        id="cycle", misfire_grace_time=60,
    )

    scheduler.add_job(
        job_emergency_check, IntervalTrigger(seconds=20),
        id="emergency_check", misfire_grace_time=15,
    )

    scheduler.add_job(
        job_daily_summary, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=5, timezone=ET,
        ),
        id="daily_summary", misfire_grace_time=300,
    )

    scheduler.add_job(
        job_maintenance, CronTrigger(
            day_of_week="mon-fri", hour=9, minute=25, timezone=ET,
        ),
        id="maintenance", misfire_grace_time=300,
    )

    logger.info("Trading service starting. Jobs: %s", [j.id for j in scheduler.get_jobs()])
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
