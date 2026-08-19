"""The always-on trading engine process for one mode ('paper' or 'live') —
run TWO instances of this (see deploy/trading-bot-paper.service and
deploy/trading-bot-live.service), each connecting to its own IB Gateway
process. Replaces Windows Task Scheduler with an internal APScheduler
running cycle.py/daily_summary.py on their cadences for this mode. The
dashboard (run_dashboard.py) is a separate process that only reads/writes
the shared SQLite DB; it never talks to IBKR directly.

The premarket prefilter scan and DB maintenance are mode-agnostic (the scan
is market data, not account data, and writes both modes' watchlists in one
pass — see morning_prefilter.py) so only the paper instance runs them, to
avoid doing the same yfinance scan twice in parallel.
"""
import argparse
import logging
import sys
import traceback
from datetime import datetime, timedelta
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


def _guarded(mode: str, job_name: str, fn, *args, **kwargs):
    """Run a scheduled job, catching and logging any exception so one bad
    tick never kills the whole service process."""
    try:
        fn(*args, **kwargs)
    except Exception:
        logger.exception("[%s] Job %s failed", mode, job_name)
        db.log_cycle_error(mode, f"[{job_name}]\n{traceback.format_exc()}")


def _run_cycle_job(scheduler: BlockingScheduler, mode: str):
    """Runs the 'cycle' job, then records the scheduler's own next firing
    time for it — this always advances every 5 minutes regardless of
    market hours (run_cycle() self-gates internally), so it's an accurate
    countdown source for the dashboard even outside trading hours."""
    _guarded(mode, "cycle", cycle.run_cycle, mode)
    job = scheduler.get_job("cycle")
    if job and job.next_run_time:
        db.set_next_cycle_at(mode, job.next_run_time.isoformat())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    args = parser.parse_args()
    mode = args.mode

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    logger.info("[%s] DB ready at %s", mode, db.DB_PATH)

    scheduler = BlockingScheduler(timezone=ET)

    scheduler.add_job(
        lambda: _run_cycle_job(scheduler, mode),
        IntervalTrigger(minutes=5),
        id="cycle", misfire_grace_time=60,
    )
    # BlockingScheduler doesn't compute next_run_time until .start() runs, so
    # seed an estimate now (accurate the moment start() actually happens, a
    # few lines below) — _run_cycle_job self-corrects it after every firing.
    db.set_next_cycle_at(mode, (datetime.now(ET) + timedelta(minutes=5)).isoformat())

    scheduler.add_job(
        lambda: _guarded(mode, "emergency_check", cycle.emergency_check, mode),
        IntervalTrigger(seconds=20),
        id="emergency_check", misfire_grace_time=15,
    )

    scheduler.add_job(
        lambda: _guarded(mode, "daily_summary", daily_summary.run, mode),
        CronTrigger(day_of_week="mon-fri", hour=16, minute=5, timezone=ET),
        id="daily_summary", misfire_grace_time=300,
    )

    scheduler.add_job(
        lambda: _guarded(mode, "account_refresh", cycle.refresh_account_info, mode),
        IntervalTrigger(minutes=5),
        id="account_refresh", misfire_grace_time=60,
        next_run_time=datetime.now(ET),  # fire once immediately, then every 5 min
    )

    if mode == "paper":
        # Shared/mode-agnostic jobs — only one instance needs to run these.
        scheduler.add_job(
            lambda: _guarded(mode, "prefilter", morning_prefilter.run_scan,
                              morning_prefilter.DEFAULT_MIN_GAP_PCT, morning_prefilter.DEFAULT_MIN_PRICE, False),
            CronTrigger(day_of_week="mon-fri", hour="9-12", minute="25,55", timezone=ET),
            id="prefilter", misfire_grace_time=120,
        )
        scheduler.add_job(
            lambda: _guarded(mode, "maintenance", db.trim_old_rows),
            CronTrigger(day_of_week="mon-fri", hour=9, minute=25, timezone=ET),
            id="maintenance", misfire_grace_time=300,
        )
        scheduler.add_job(
            lambda: _guarded(mode, "watchlist_filters", cycle.scan_watchlist_filters),
            IntervalTrigger(minutes=5),
            id="watchlist_filters", misfire_grace_time=60,
        )

    logger.info("[%s] Trading service starting. Jobs: %s", mode, [j.id for j in scheduler.get_jobs()])
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[%s] Shutting down", mode)


if __name__ == "__main__":
    main()
