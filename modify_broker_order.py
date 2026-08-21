"""Places or moves a protective stop or take-profit (limit) order for ANY
real IBKR holding — independent of whether the bot itself opened it or is
tracking it in its own `positions` table — spawned as a subprocess by the
dashboard (web/app.py's PUT /api/broker_positions/{symbol}/order). Runs on
its own IBKR client ID so it never collides with the orchestrator's
connection or any of the other dashboard-spawned scripts (trade.py,
close_position.py, modify_stop.py). --mode selects which IB Gateway
process (paper on 4002, live on 4001) it connects to.

Deliberately does NOT touch this bot's own `positions` table even when the
symbol happens to also be a bot-tracked position: this endpoint edits the
account's real resting orders directly (matching Account Holdings' own
"independent of the bot" model), not the bot's internal tracking — see
modify_stop.py instead for editing a bot-tracked position's own stop.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values
from ib_async import LimitOrder, Stock, StopOrder

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
    parser.add_argument("--order-type", required=True, choices=["stop", "take_profit"])
    parser.add_argument("--price", required=True, type=float)
    args = parser.parse_args()
    symbol = args.symbol.upper()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_ORDER_CLIENT_ID", 15)),
    )
    try:
        ib = ibkr.ib
        ib.sleep(1)  # let positions populate after connecting
        held = next((p for p in ib.positions() if p.contract.symbol == symbol and p.position != 0), None)
        if held is None:
            print(f"[{args.mode}] {symbol}: no open position in this account")
            sys.exit(1)

        qty = int(abs(held.position))
        # A stop/take-profit protects/exits a long with a SELL, a short
        # with a BUY - same convention as cycle._place_stop.
        action = "SELL" if held.position > 0 else "BUY"

        # Cancel any existing order of the SAME type for this symbol before
        # placing the new one (cancel-then-replace, same as modify_stop.py)
        # - reqAllOpenOrders pulls in orders from other client IDs too, so
        # this also catches one the bot's own cycle placed.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        target_type = "STP" if args.order_type == "stop" else "LMT"
        for t in ib.openTrades():
            if t.contract.symbol == symbol and t.order.orderType == target_type:
                ib.cancelOrder(t.order)
        ib.sleep(1)

        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)
        order = (
            StopOrder(action, qty, round(args.price, 2)) if args.order_type == "stop"
            else LimitOrder(action, qty, round(args.price, 2))
        )
        trade = ib.placeOrder(qualified, order)
        ib.sleep(1)

        print(f"[{args.mode}] {symbol}: {args.order_type} set to ${args.price:.2f} x{qty} (order_id={trade.order.orderId})")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
