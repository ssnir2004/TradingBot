"""Manual close for any real IBKR holding, spawned as a subprocess by the
dashboard (web/app.py's /api/broker_positions/close) — independent of
whether the bot itself opened or is tracking the position. Runs on its own
IBKR client ID (IBKR_CLOSE_CLIENT_ID) so it never collides with the
orchestrator's connection or trade.py's. --mode selects which IB Gateway
process (paper on 4002, live on 4001) it connects to.
"""
import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from src import db, mode_config
from src.ibkr_client import IBKRClient, scoped_positions

PROJECT_DIR = Path(__file__).resolve().parent


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--qty", type=int, default=None, help="Shares to close; omit to close the full position.")
    args = parser.parse_args()
    symbol = args.symbol.upper()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    env = dotenv_values(PROJECT_DIR / ".env")

    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_CLOSE_CLIENT_ID", 13)),
        account=mode_config.ibkr_account(env, account_id, args.mode),
    )

    try:
        ib = ibkr.ib
        ib.sleep(1)  # let positions populate after connecting
        held = next((p for p in scoped_positions(ib) if p.contract.symbol == symbol and p.position != 0), None)
        if held is None:
            print(f"[{args.mode}] {symbol}: no open position in this account")
            sys.exit(1)

        action = "SELL" if held.position > 0 else "BUY"
        close_qty = int(abs(held.position)) if args.qty is None else min(args.qty, int(abs(held.position)))
        if close_qty < 1:
            print(f"[{args.mode}] {symbol}: nothing to close (qty={close_qty})")
            sys.exit(1)

        trade = ibkr.place_order(symbol, action, close_qty)
        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice or 0
        order_id = trade.order.orderId
        # See trade.py's identical comment - lets cycle.sync_broker_fills
        # recognize this exact fill later and skip re-logging it.
        exec_id = trade.fills[-1].execution.execId if trade.fills else None

        db.record_trade(account_id, args.mode, symbol, action, close_qty, fill_price, order_id, status, exec_id=exec_id)

        if status != "Filled" or fill_price <= 0:
            for entry in trade.log:
                print(f"trade.log: {entry}")
            print(f"[{args.mode}] {action} {close_qty} {symbol}: order_id={order_id} "
                  f"fill_price={fill_price} status={status} (not filled)")
            sys.exit(1)

        # If the bot itself was tracking this symbol (own entry, own stop),
        # a manual close from here bypasses that entirely — cancel the
        # orphaned stop and drop the row so the bot doesn't keep "managing"
        # a position that no longer exists.
        closed_side = "long" if action == "SELL" else "short"
        for pos in db.get_open_positions(account_id, args.mode):
            if pos["symbol"] != symbol or pos["side"] != closed_side:
                continue
            stop_order_id = pos.get("stop_order_id")
            if stop_order_id is not None:
                for t in ib.trades():
                    if t.order.orderId == stop_order_id:
                        ib.cancelOrder(t.order)
            db.remove_position(account_id, args.mode, symbol)

        print(f"[{args.mode}] {action} {close_qty} {symbol}: order_id={order_id} "
              f"fill_price={fill_price} status={status}")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
