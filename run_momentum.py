"""Always-on momentum process (see deploy/momentum-scan.service).

Isolated from run_service.py: its own APScheduler, its own SQLite tables,
its own IBKR client id when phase 3 is armed. Safe to run alongside the
live S&P engine - they share only the DB file (and, only once phase 3
places an order, the same broker account - never the same position).

Real order placement is gated ENTIRELY by momentum.config's live.enabled
(the dashboard's own kill switch), not by a process launch flag - this
process always runs both the scan/alert loop and the position-management
job; management is a cheap no-op whenever there are zero open positions,
which is always true while live.enabled is False. The --exec flag below
is kept only so the existing systemd unit's ExecStart line doesn't need
editing; it no longer gates anything.

  python run_momentum.py                  # scan+alert always on; live execution follows momentum.config's own switch
  python run_momentum.py --once           # run one scan cycle now (ignores the entry window) and exit
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


def _scan_cycle(account_id: int):
    try:
        summary = run_scan_cycle(account_id)
        if summary.get("skipped"):
            return
        log.info("scan: %s candidates, %s shortlist, %s signals %s",
                 summary.get("candidates"), summary.get("shortlist"),
                 summary.get("signals"), summary.get("fired") or "")
    except Exception:
        log.exception("momentum scan cycle failed")


def _manage_cycle():
    """Cheap no-op when nothing is open (store.get_open_positions() short-
    circuits) - safe to schedule unconditionally regardless of whether
    live trading is currently armed."""
    try:
        cfg = load_config()
        from momentum import live
        live.manage_open_positions(cfg)
    except Exception:
        log.exception("momentum position management cycle failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--account-id", type=int, default=None)
    ap.add_argument("--exec", dest="exec_mode", choices=["alert", "live"], default="alert",
                    help="vestigial - kept for the existing systemd unit's ExecStart line. "
                         "Real order placement is controlled by momentum.config's live.enabled "
                         "(the dashboard's LIVE TRADING switch), checked fresh every cycle - not this flag.")
    ap.add_argument("--once", action="store_true", help="run one scan cycle now and exit")
    args = ap.parse_args()

    db.init_db()
    store.init_momentum_db()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    cfg = load_config()
    log.info("momentum up. account=%s poll=%ss window=%s live_kill_switch=%s live_strategies=%s",
             account_id, cfg["screener"]["poll_seconds"], cfg["screener"]["entry_window_et"],
             cfg["live"]["enabled"], cfg["live_strategies"])

    if args.once:
        summary = run_scan_cycle(account_id, force=True)
        log.info("once: %s", summary)
        return

    sched = BlockingScheduler(timezone=ET)
    sched.add_job(lambda: _scan_cycle(account_id),
                  IntervalTrigger(seconds=cfg["screener"]["poll_seconds"]),
                  id="momentum_scan", misfire_grace_time=30, max_instances=1,
                  next_run_time=datetime.now(ET))
    sched.add_job(_manage_cycle,
                  IntervalTrigger(seconds=cfg["live"]["management_poll_seconds"]),
                  id="momentum_manage_positions", misfire_grace_time=15, max_instances=1)
    sched.add_job(lambda: store.trim_old_rows(),
                  IntervalTrigger(hours=12), id="momentum_trim", misfire_grace_time=3600)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("shutting down")


if __name__ == "__main__":
    main()
