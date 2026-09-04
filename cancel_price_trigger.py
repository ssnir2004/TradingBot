"""Cancels a still-pending "buy line"/"sell line" entry trigger (see
place_price_trigger.py), spawned as a subprocess by the dashboard
(web/app.py's DELETE /api/price_triggers/{trigger_id}). Runs on its own
IBKR client ID so it never collides with any other connection.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    parser.add_argument("--trigger-id", required=True, type=int)
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    trig = db.get_price_trigger(account_id, args.mode, args.trigger_id)
    if trig is None or trig["status"] != "pending":
        print(f"[{args.mode}] trigger {args.trigger_id}: not a pending trigger")
        sys.exit(1)

    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_CANCEL_TRIGGER_CLIENT_ID", 18)),
        account=mode_config.ibkr_account(env, account_id, args.mode),
    )
    try:
        ib = ibkr.ib
        order = next((t.order for t in ib.trades() if t.order.orderId == trig["broker_order_id"]), None)
        if order is not None:
            ib.cancelOrder(order)
            ib.sleep(1)

        db.resolve_price_trigger(account_id, args.mode, args.trigger_id, "cancelled")
        print(f"[{args.mode}] {trig['symbol']}: trigger {args.trigger_id} cancelled")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
