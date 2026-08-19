"""Order execution script, spawned as a subprocess by bot.py and cycle.py.
Runs on its own IBKR client ID (IBKR_EXEC_CLIENT_ID) so it never collides
with the orchestrator's connection.
"""
import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values

from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
TRADES_CSV = PROJECT_DIR / "trades.csv"
TRADES_HEADER = ["timestamp_iso", "symbol", "side", "size", "fill_price", "order_id", "status"]
FAILED_STATUSES = {"Cancelled", "ApiCancelled", "Inactive"}


def _ensure_trades_csv():
    if not TRADES_CSV.exists():
        with open(TRADES_CSV, "w", newline="") as f:
            csv.writer(f).writerow(TRADES_HEADER)


def _append_trade_row(symbol, side, size, fill_price, order_id, status):
    _ensure_trades_csv()
    with open(TRADES_CSV, "a", newline="") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            symbol, side, size, fill_price, order_id, status,
        ])


def main():
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

        _append_trade_row(args.symbol, args.side, args.size, fill_price, order_id, status)

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
