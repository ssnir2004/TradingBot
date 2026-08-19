"""Order execution script, spawned as a subprocess by bot.py and cycle.py.
Runs on its own IBKR client ID (IBKR_EXEC_CLIENT_ID) so it never collides
with the orchestrator's connection.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from src import db
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
FAILED_STATUSES = {"Cancelled", "ApiCancelled", "Inactive"}


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["BUY", "SELL"])
    parser.add_argument("--size", required=True, type=int)
    args = parser.parse_args()

    env = dotenv_values(PROJECT_DIR / ".env")

    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        int(env.get("IBKR_PORT", 7497)),
        int(env.get("IBKR_EXEC_CLIENT_ID", 3)),
    )

    try:
        trade = ibkr.place_order(args.symbol, args.side, args.size)
        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice or 0
        order_id = trade.order.orderId

        db.record_trade(args.symbol, args.side, args.size, fill_price, order_id, status)

        if status in FAILED_STATUSES:
            for entry in trade.log:
                print(f"trade.log: {entry}")
            sys.exit(1)

        print(f"{args.side} {args.size} {args.symbol}: order_id={order_id} "
              f"fill_price={fill_price} status={status}")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
