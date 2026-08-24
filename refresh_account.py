"""Refreshes net liquidation/cash/buying power, broker positions, and open
orders for the dashboard's Account Holdings view - spawned as a subprocess
by the dashboard (web/app.py's POST /api/account/refresh) so the on-demand
"Refresh now" button does exactly what run_service.py's scheduler already
does every 5 minutes, just right now instead of waiting for the next tick.
Reuses cycle.refresh_account_info (and its own ACCOUNT_REFRESH_CLIENT_ID)
rather than duplicating that logic - the scheduled job and this on-demand
trigger are the same operation, so a rare overlap just means one of the
two gets a "client id in use" error and finishes normally on its own; not
worth a second dedicated client ID for.
"""
import argparse
import sys
from pathlib import Path

import cycle
from src import db

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    try:
        cycle.refresh_account_info(account_id, args.mode)
    except Exception as exc:  # noqa: BLE001 - dashboard subprocess: report, don't traceback
        print(f"[{args.mode}] refresh failed: {exc}")
        sys.exit(1)
    print(f"[{args.mode}] account refreshed")


if __name__ == "__main__":
    main()
