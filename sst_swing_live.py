"""SST Swing's own once-daily scheduled job (source prompt section 8;
G-SST-6) - entry evaluation and Rule 6's trailing-stop update, on DAILY
bars, run once per day (see run_service.py's own scheduling comment for
the exact time - shortly after 09:35 ET, once the prior session's daily
bar is fully closed) instead of from run_cycle's per-minute loop, which
explicitly skips this strategy family entirely (see cycle.entry_scan's
own docstring).

All the actual decision/execution logic lives in cycle.py (sst_entry_
scan, virtual_sst_entry_scan, sst_manage_positions, sst_manage_virtual_
positions) and src/sst_swing.py (the six rules themselves) - this script
is a thin driver, same relationship refresh_account.py has to cycle.
refresh_account_info. Reuses the exact same real-order-placement layer
every other strategy already uses (trade.py, _place_stop) rather than a
second, parallel one, per the source prompt's own explicit instruction -
this is NOT a standalone execution path.

Disabled by default the same way every other strategy already is: an
SST Swing strategy's strategy_run.run_mode starts at 'off' until
switched to 'virtual' or 'live' from the Strategies screen, the same
mechanism (and same guardrail) the rest of the multi-strategy engine
already provides - no SST-specific "enable live" flag needed.
"""
import argparse
import json
import sys
from pathlib import Path

from dotenv import dotenv_values

import cycle
from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent


def run_daily_scan(account_id: int, mode: str) -> dict:
    env = dotenv_values(PROJECT_DIR / ".env")
    try:
        ibkr = IBKRClient(
            env.get("IBKR_HOST", "127.0.0.1"),
            mode_config.ibkr_port(env, account_id, mode),
            cycle.SST_SWING_CLIENT_ID,
            account=mode_config.ibkr_account(env, account_id, mode),
        )
    except Exception as exc:  # noqa: BLE001 - report, don't traceback, same as refresh_account.py
        print(f"[{mode}] SST daily scan: IBKR connect failed: {type(exc).__name__}: {exc}")
        return {"success": False, "error": str(exc)}

    entries_attempted = 0
    trails_checked = 0
    try:
        ib = ibkr.ib
        positions = db.get_open_positions(account_id, mode)

        # Rule 6 first, same "manage what's already open before scanning
        # for new entries" ordering run_cycle's own Step 3/4-before-Step-8
        # already follows.
        cycle.sst_manage_positions(account_id, mode, ib, positions)

        for strategy_run in db.list_strategy_runs(account_id):
            if strategy_run["run_mode"] == "off":
                continue
            strategy = db.get_strategy(strategy_run["strategy_id"])
            if strategy is None:
                continue
            rules = json.loads(strategy["rules_json"])
            if rules.get("strategy_type") != "sst_swing":
                continue
            side = strategy["direction"]

            if strategy_run["run_mode"] == "live":
                positions = cycle.sst_entry_scan(account_id, mode, ib, positions, rules, side, strategy_run)
                entries_attempted += 1
            elif strategy_run["run_mode"] == "virtual":
                cycle.virtual_sst_entry_scan(account_id, mode, rules, side, strategy_run)
                cycle.sst_manage_virtual_positions(account_id, strategy_run)
                entries_attempted += 1
            trails_checked += 1

        db.record_cycle_run(account_id, mode, "sst_daily_scan")
        return {"success": True, "strategy_runs_scanned": trails_checked, "entry_scans_run": entries_attempted}
    except Exception as exc:  # noqa: BLE001
        import traceback
        db.log_cycle_error(account_id, mode, traceback.format_exc())
        from src.notify import notify
        notify(f"[{mode.upper()}] SST daily scan CRASHED", str(exc)[:500], "high")
        return {"success": False, "error": str(exc)}
    finally:
        ibkr.disconnect()


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="live")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    result = run_daily_scan(account_id, args.mode)
    print(result)
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
