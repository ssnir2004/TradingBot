"""Adds or cancels a protective stop or take-profit (limit) order for ANY
real IBKR holding — independent of whether the bot itself opened it or is
tracking it in its own `positions` table — spawned as a subprocess by the
dashboard (web/app.py's POST/DELETE /api/broker_positions/{symbol}/order*).
Runs on its own IBKR client ID so it never collides with the orchestrator's
connection or any of the other dashboard-spawned scripts (trade.py,
close_position.py, modify_stop.py). --mode selects which IB Gateway
process (paper on 4002, live on 4001) it connects to.

A symbol can carry several stop and/or take-profit orders at once, each
covering its own slice of the position (scaling out) — this script only
ever adds a brand-new order or cancels one specific existing order by ID;
"moving" an order is the dashboard doing a cancel followed by an add.

Deliberately does NOT touch this bot's own `positions` table even when the
symbol happens to also be a bot-tracked position: this edits the account's
real resting orders directly (matching Account Holdings' own "independent
of the bot" model), not the bot's internal tracking — see modify_stop.py
instead for editing a bot-tracked position's own single stop.
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
    parser.add_argument("--action", required=True, choices=["add", "cancel"])
    parser.add_argument("--order-type", choices=["stop", "take_profit"], help="Required for --action add.")
    parser.add_argument("--price", type=float, help="Required for --action add.")
    parser.add_argument("--qty", type=int, help="Required for --action add.")
    parser.add_argument("--order-id", type=int, help="Required for --action cancel.")
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
        ib.sleep(1)  # let positions/orders populate after connecting

        if args.action == "cancel":
            if args.order_id is None:
                print(f"[{args.mode}] --order-id is required for --action cancel")
                sys.exit(1)
            ib.reqAllOpenOrders()
            ib.sleep(1)
            match = next((t for t in ib.openTrades() if t.order.orderId == args.order_id), None)
            if match is None:
                print(f"[{args.mode}] {symbol}: order {args.order_id} not found among open orders "
                      f"(already filled or cancelled?)")
                sys.exit(1)
            ib.cancelOrder(match.order)
            print(f"[{args.mode}] {symbol}: order {args.order_id} cancelled")
            return

        # action == "add"
        if args.order_type is None or args.price is None or args.qty is None:
            print(f"[{args.mode}] --order-type, --price, and --qty are all required for --action add")
            sys.exit(1)

        held = next((p for p in ib.positions() if p.contract.symbol == symbol and p.position != 0), None)
        if held is None:
            print(f"[{args.mode}] {symbol}: no open position in this account")
            sys.exit(1)
        held_qty = int(abs(held.position))

        # A stop/take-profit protects/exits a long with a SELL, a short
        # with a BUY - same convention as cycle._place_stop.
        action = "SELL" if held.position > 0 else "BUY"

        # A full stop plus a full take-profit on the same shares is the
        # normal bracket pattern (protect the position both ways at once -
        # these aren't OCO-linked, so if one fills the other is simply left
        # to cancel manually), so only orders of the SAME type going the
        # SAME closing direction compete for the share count: two stops
        # together can't cover more than what's held, but a stop and a
        # take-profit both can. An order going the other way (e.g. a
        # separate limit buy unrelated to exiting this position) doesn't
        # count against this at all.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        target_type = "STP" if args.order_type == "stop" else "LMT"
        allocated = sum(
            int(t.order.totalQuantity) for t in ib.openTrades()
            if t.contract.symbol == symbol and t.order.orderType == target_type and t.order.action == action
        )
        if allocated + args.qty > held_qty:
            print(f"[{args.mode}] {symbol}: {allocated} share(s) already allocated across existing "
                  f"{args.order_type} orders - adding {args.qty} more would exceed the {held_qty} actually held")
            sys.exit(1)

        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)
        order = (
            StopOrder(action, args.qty, round(args.price, 2)) if args.order_type == "stop"
            else LimitOrder(action, args.qty, round(args.price, 2))
        )
        trade = ib.placeOrder(qualified, order)
        ib.sleep(1)

        print(f"[{args.mode}] {symbol}: {args.order_type} added: {args.qty} @ ${args.price:.2f} "
              f"(order_id={trade.order.orderId})")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
