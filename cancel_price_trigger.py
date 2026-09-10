"""Cancels a still-pending "buy line"/"sell line" entry trigger (see
place_price_trigger.py), spawned as a subprocess by the dashboard
(web/app.py's DELETE /api/price_triggers/{trigger_id}). Runs on its own
IBKR client ID so it never collides with any other connection.

Real live-money incident (2026-09-09/10): this script used to look the
order up via ib.trades() and unconditionally mark it 'cancelled' in our
DB regardless of what it found - but ib.trades() only ever reflects
orders THIS connection's own client id placed or was told about, and the
real trigger order was placed by place_price_trigger.py's own, DIFFERENT
client id, so the lookup always found nothing. A UNH/TSCO trigger had
each already filled for real at the broker by the time a stale cancel
click ran this script; it silently marked them 'cancelled' anyway, which
meant cycle.check_price_triggers' own fill-detection was never going to
pick them up again either - both positions were left completely
untracked and unprotected (no stop) with no record of what happened.
Fixed to check for a real fill FIRST (via reqExecutions, account-wide,
unlike ib.trades()) and refuse to cancel if one is found - see the error
message below for what to do instead.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from src import db, mode_config
from src.ibkr_client import IBKRClient, belongs_to_account, cancel_order_any_client

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
        order_id = trig["broker_order_id"]

        # Account-wide (reqExecutions, not ib.trades()) - see this file's
        # own module docstring for why this check exists at all. Also
        # requires the fill's own symbol to match trig's - a colliding
        # orderId across two genuinely different real orders (a separate
        # live incident, 2026-09-10, CHTR/TSCO - see check_price_triggers'
        # own docstring) could otherwise match an unrelated symbol's fill
        # here and wrongly refuse to cancel THIS trigger.
        for fill in ib.reqExecutions():
            if fill.execution.orderId == order_id and fill.contract.symbol == trig["symbol"] and belongs_to_account(ib, fill.execution.acctNumber):
                print(f"[{args.mode}] {trig['symbol']}: trigger {args.trigger_id} already filled at the broker "
                      f"({fill.execution.shares} @ {fill.execution.price}) - refusing to cancel. "
                      f"The bot's own cycle will pick up the fill and start managing it on its next tick.")
                sys.exit(1)

        # Account-wide (reqAllOpenOrders/openTrades, not ib.trades()) -
        # same reasoning, same symbol guard.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        match = next((t.order for t in ib.openTrades() if t.order.orderId == order_id and t.contract.symbol == trig["symbol"] and belongs_to_account(ib, t.order.account)), None)
        if match is not None:
            # place_price_trigger.py placed this under its own, different
            # client id - a plain ib.cancelOrder() here would silently
            # fail (see cancel_order_any_client's own docstring for the
            # real live incident, 2026-09-10, that class of bug caused).
            cancel_order_any_client(ib, match)

        db.resolve_price_trigger(account_id, args.mode, args.trigger_id, "cancelled")
        print(f"[{args.mode}] {trig['symbol']}: trigger {args.trigger_id} cancelled")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
