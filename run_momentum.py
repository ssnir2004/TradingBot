"""Always-on momentum scanner process (see deploy/momentum-scan.service).

Isolated from run_service.py: no IBKR connection in phase 1 (bars come
from yfinance, exactly like cycle.py's own intraday path), its own
APScheduler, its own SQLite tables. Safe to run alongside the live S&P
engine - they share only the DB file.

  python run_momentum.py --exec alert     # phase 1 default: scan + alert, no orders
  python run_momentum.py --once           # run one cycle now (ignores the entry window) and exit
"""
import argparse
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src import db
from momentum import store
from momentum.config import load_config
from momentum.loop import run_scan_cycle

ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("run_momentum")


def _cycle(account_id: int, exec_mode: str):
    try:
        summary = run_scan_cycle(account_id)
        if summary.get("skipped"):
            return
        log.info("scan: %s candidates, %s shortlist, %s signals %s",
                 summary.get("candidates"), summary.get("shortlist"),
                 summary.get("signals"), summary.get("fired") or "")
    except Exception:
        log.exception("momentum cycle failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--account-id", type=int, default=None)
    ap.add_argument("--exec", dest="exec_mode", choices=["alert", "live"], default="alert",
                    help="phase 1 supports 'alert' only")
    ap.add_argument("--once", action="store_true", help="run one cycle now and exit")
    args = ap.parse_args()

    if args.exec_mode == "live":
        log.error("--exec live is not implemented in phase 1. Use 'alert'.")
        sys.exit(2)

    db.init_db()
    store.init_momentum_db()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    cfg = load_config()
    log.info("momentum scanner up. account=%s exec=%s poll=%ss window=%s",
             account_id, args.exec_mode, cfg["screener"]["poll_seconds"],
             cfg["screener"]["entry_window_et"])

    if args.once:
        summary = run_scan_cycle(account_id, force=True)
        log.info("once: %s", summary)
        return

    sched = BlockingScheduler(timezone=ET)
    sched.add_job(lambda: _cycle(account_id, args.exec_mode),
                  IntervalTrigger(seconds=cfg["screener"]["poll_seconds"]),
                  id="momentum_scan", misfire_grace_time=30, max_instances=1,
                  next_run_time=datetime.now(ET))
    sched.add_job(lambda: store.trim_old_rows(),
                  IntervalTrigger(hours=12), id="momentum_trim", misfire_grace_time=3600)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("shutting down")


if __name__ == "__main__":
    main()
