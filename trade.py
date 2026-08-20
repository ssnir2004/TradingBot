"""Order execution script, spawned as a subprocess by bot.py and cycle.py.
Runs on its own IBKR client ID (IBKR_EXEC_CLIENT_ID) so it never collides
with the orchestrator's connection. --mode selects which IB Gateway
process (paper on 4002, live on 4001) it connects to.
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
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["BUY", "SELL"])
    parser.add_argument("--size", required=True, type=int)
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    env = dotenv_values(PROJECT_DIR / ".env")

    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, args.mode),
        int(env.get("IBKR_EXEC_CLIENT_ID", 3)),
    )

    try:
        trade = ibkr.place_order(args.symbol, args.side, args.size)
        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice or 0
        order_id = trade.order.orderId

        db.record_trade(account_id, args.mode, args.symbol, args.side, args.size, fill_price, order_id, status)

        # Success is defined by what actually happened, not by absence from
        # a denylist: only a real fill counts. Anything else (Cancelled,
        # Inactive, a ValidationError that never resolved before the
        # polling deadline, or even a live-but-unfilled Submitted) must
        # fail loudly here — a caller (cycle.py's entry_scan) treats a
        # non-zero exit as "no position was opened", and a false success
        # here would have it record a phantom position for a share that
        # was never actually bought.
        if status != "Filled" or fill_price <= 0:
            for entry in trade.log:
                print(f"trade.log: {entry}")
            print(f"[{args.mode}] {args.side} {args.size} {args.symbol}: order_id={order_id} "
                  f"fill_price={fill_price} status={status} (not filled)")
            sys.exit(1)

        print(f"[{args.mode}] {args.side} {args.size} {args.symbol}: order_id={order_id} "
              f"fill_price={fill_price} status={status}")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
