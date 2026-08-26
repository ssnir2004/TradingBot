"""Moves the protective stop for a bot-tracked open position to a new
price, spawned as a subprocess by the dashboard (web/app.py's PUT
/api/positions/{symbol}/stop). Runs on its own IBKR client ID so it never
collides with the orchestrator's connection, trade.py's, or
close_position.py's. --mode selects which IB Gateway process (paper on
4002, live on 4001) it connects to.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values
from ib_async import Stock, StopOrder

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
    parser.add_argument("--stop-price", required=True, type=float)
    args = parser.parse_args()
    symbol = args.symbol.upper()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    pos = next((p for p in db.get_open_positions(account_id, args.mode) if p["symbol"] == symbol), None)
    if pos is None:
        print(f"[{args.mode}] {symbol}: not a bot-tracked open position")
        sys.exit(1)

    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_MODIFY_CLIENT_ID", 14)),
        account=mode_config.ibkr_account(env, account_id, args.mode),
    )
    try:
        ib = ibkr.ib
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)

        # Cancel the existing stop (if the API can still find it) before
        # placing the new one - same cancel-then-replace sequence cycle.py
        # itself uses for breakeven flips and trailing-stop updates.
        old_order_id = pos.get("stop_order_id")
        if old_order_id is not None:
            for t in ib.trades():
                if t.order.orderId == old_order_id:
                    ib.cancelOrder(t.order)
            ib.sleep(1)

        action = "SELL" if pos.get("side", "long") == "long" else "BUY"
        order = StopOrder(action, pos["qty"], round(args.stop_price, 2))
        if getattr(ib, "account", None):
            order.account = ib.account
        trade = ib.placeOrder(qualified, order)
        ib.sleep(1)
        new_order_id = trade.order.orderId

        pos["stop_price"] = args.stop_price
        pos["stop_order_id"] = new_order_id
        db.upsert_position(account_id, args.mode, pos)

        print(f"[{args.mode}] {symbol}: stop moved to ${args.stop_price:.2f} (order_id={new_order_id})")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
