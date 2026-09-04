"""Places a real native IBKR STP order behind a user-set "buy line"/"sell
line" from the dashboard's chart screen (web/app.py's POST /api/price_
triggers), then records it in the price_triggers table. Runs on its own
IBKR client ID (IBKR_PRICE_TRIGGER_CLIENT_ID) so it never collides with
any other connection.

This is IBKR's own server-side stop order, not something this bot polls
price for and fires itself: a BUY stop above the current market opens a
long once price trades up to trigger_price ("buy line"); a SELL stop
below the current market opens a fresh short once price trades down to
trigger_price ("sell line"). stop_price is only recorded here, not placed
yet - it becomes this position's real protective stop the moment the
entry itself fills (see cycle.check_price_triggers, which every cycle
tick checks this order's status and, on a fill, places that stop and
starts bot management exactly like any strategy-opened position)."""
import argparse
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from ib_async import Stock, StopOrder

from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["long", "short"])
    parser.add_argument("--trigger-price", required=True, type=float)
    parser.add_argument("--stop-price", required=True, type=float)
    parser.add_argument("--qty", required=True, type=int)
    parser.add_argument("--created-by", default="")
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    symbol = args.symbol.strip().upper()

    if args.qty <= 0:
        print("qty must be positive")
        sys.exit(1)
    if args.trigger_price <= 0 or args.stop_price <= 0:
        print("trigger_price and stop_price must be positive")
        sys.exit(1)
    # A long's protective stop must sit below its entry, a short's above -
    # same direction check entry_scan implicitly enforces via r <= 0.
    if args.side == "long" and args.stop_price >= args.trigger_price:
        print("for a buy line (side=long), stop_price must be below trigger_price")
        sys.exit(1)
    if args.side == "short" and args.stop_price <= args.trigger_price:
        print("for a sell line (side=short), stop_price must be above trigger_price")
        sys.exit(1)

    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_PRICE_TRIGGER_CLIENT_ID", 17)),
        account=mode_config.ibkr_account(env, account_id, args.mode),
    )
    try:
        ib = ibkr.ib
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)

        # A BUY stop above the market opens a long ("buy line"); a SELL
        # stop below the market opens a fresh short ("sell line") - the
        # opposite action mapping from a protective stop on an EXISTING
        # position (modify_stop.py, cycle._place_stop), since this instead
        # OPENS a brand-new one in `side`'s direction.
        action = "BUY" if args.side == "long" else "SELL"
        order = StopOrder(action, args.qty, round(args.trigger_price, 2))
        if getattr(ib, "account", None):
            order.account = ib.account
        trade = ib.placeOrder(qualified, order)
        ib.sleep(1)
        order_id = trade.order.orderId

        db.create_price_trigger(account_id, args.mode, {
            "symbol": symbol, "side": args.side, "trigger_price": args.trigger_price,
            "stop_price": args.stop_price, "qty": args.qty, "broker_order_id": order_id,
            "created_at": datetime.now(ET).isoformat(timespec="seconds"), "created_by": args.created_by,
        })
        print(f"[{args.mode}] {symbol}: {args.side} trigger placed @ ${args.trigger_price:.2f} "
              f"(order_id={order_id}), stop ${args.stop_price:.2f} once filled, qty {args.qty}")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
