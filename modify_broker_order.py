"""Adds, edits, or cancels resting orders for ANY real IBKR holding or
pending order — independent of whether the bot itself opened it or is
tracking it in its own `positions` table — spawned as a subprocess by the
dashboard (web/app.py's POST/PUT/DELETE /api/broker_positions/{symbol}/
order* and /api/orders/*). Runs on its own IBKR client ID so it never
collides with the orchestrator's connection or any of the other
dashboard-spawned scripts (trade.py, close_position.py, modify_stop.py).
--mode selects which IB Gateway process (paper on 4002, live on 4001) it
connects to.

A symbol can carry several stop and/or take-profit orders at once, each
covering its own slice of the position (scaling out) — --action add
always creates a brand-new order rather than touching an existing one.
An --order-type of "stop" or "take_profit" adds a plain STP/LMT order at
--price; "atr_trailing_stop" instead adds a real IBKR TRAIL order sized
from a fresh ATR read (--atr-period/--atr-multiplier), the same ATR
bracket-stop math open_position.py uses for a brand-new entry, but for a
position that's already open.
--action edit modifies ONE existing order (any type — LMT entry, STP,
TRAIL, take-profit) in place by resubmitting with its same order ID,
which IBKR treats as a live modification rather than cancel+replace.
--action cancel works on any order type/ID too, not just stop/take-profit.

Deliberately does NOT touch this bot's own `positions` table even when the
symbol happens to also be a bot-tracked position: this edits the account's
real resting orders directly (matching Account Holdings' own "independent
of the bot" model), not the bot's internal tracking — see modify_stop.py
instead for editing a bot-tracked position's own single stop.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values
from ib_async import LimitOrder, Order, Stock, StopOrder

from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
# Same ATR bracket-stop math as open_position.py, reused here so an
# ATR trailing stop added to an already-open position is computed
# identically to one placed at entry time.
ATR_BAR_SIZE = "5 mins"
ATR_LOOKBACK_DAYS = 10
REQUEST_TIMEOUT_SECONDS = 25  # stay well under web/app.py's SUBPROCESS_TIMEOUT (40s)


def _atr(df: pd.DataFrame, length: int) -> float:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return float(tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean().iloc[-1])


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--action", required=True, choices=["add", "edit", "cancel"])
    parser.add_argument("--order-type", choices=["stop", "take_profit", "atr_trailing_stop"], help="Required for --action add.")
    parser.add_argument("--price", type=float, help="Required for --action add of stop/take_profit; new price for --action edit.")
    parser.add_argument("--qty", type=int, help="Required for --action add; new qty for --action edit.")
    parser.add_argument("--order-id", type=int, help="Required for --action edit/cancel.")
    parser.add_argument("--atr-period", type=int, default=14, help="Used for --order-type atr_trailing_stop.")
    parser.add_argument("--atr-multiplier", type=float, help="Required for --order-type atr_trailing_stop.")
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

        if args.action == "edit":
            if args.order_id is None or args.price is None:
                print(f"[{args.mode}] --order-id and --price are required for --action edit")
                sys.exit(1)
            ib.reqAllOpenOrders()
            ib.sleep(1)
            match = next((t for t in ib.openTrades() if t.order.orderId == args.order_id), None)
            if match is None:
                print(f"[{args.mode}] {symbol}: order {args.order_id} not found among open orders "
                      f"(already filled or cancelled?)")
                sys.exit(1)
            order = match.order
            # STP and TRAIL both carry their distance/price in auxPrice -
            # only a plain LMT order uses lmtPrice (see cycle.py's own
            # refresh_account_info, which reads the same field this way).
            if order.orderType in ("STP", "TRAIL"):
                order.auxPrice = round(args.price, 2)
            else:
                order.lmtPrice = round(args.price, 2)
            if args.qty is not None:
                order.totalQuantity = args.qty
            ib.placeOrder(match.contract, order)
            ib.sleep(1)
            print(f"[{args.mode}] {symbol}: order {args.order_id} ({order.orderType}) updated to "
                  f"${args.price:.2f}" + (f", qty {args.qty}" if args.qty is not None else ""))
            return

        # action == "add"
        if args.order_type is None or args.qty is None:
            print(f"[{args.mode}] --order-type and --qty are required for --action add")
            sys.exit(1)
        if args.order_type == "atr_trailing_stop":
            if args.atr_multiplier is None:
                print(f"[{args.mode}] --atr-multiplier is required for --order-type atr_trailing_stop")
                sys.exit(1)
        elif args.price is None:
            print(f"[{args.mode}] --price is required for --order-type stop/take_profit")
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
        # to cancel manually), so only orders of the SAME class going the
        # SAME closing direction compete for the share count: two stops
        # together can't cover more than what's held, but a stop and a
        # take-profit both can. A plain stop and an ATR trailing stop are
        # the same class here (STP and TRAIL both just protect the
        # position, one at a fixed price and one that follows it) - they
        # compete against each other for the share count too, not just
        # against their own exact order type. An order going the other way
        # (e.g. a separate limit buy unrelated to exiting this position)
        # doesn't count against this at all.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        target_types = ("STP", "TRAIL") if args.order_type in ("stop", "atr_trailing_stop") else ("LMT",)
        allocated = sum(
            int(t.order.totalQuantity) for t in ib.openTrades()
            if t.contract.symbol == symbol and t.order.orderType in target_types and t.order.action == action
        )
        if allocated + args.qty > held_qty:
            print(f"[{args.mode}] {symbol}: {allocated} share(s) already allocated across existing "
                  f"{args.order_type} orders - adding {args.qty} more would exceed the {held_qty} actually held")
            sys.exit(1)

        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)

        if args.order_type == "atr_trailing_stop":
            bars = ib.reqHistoricalData(
                qualified, endDateTime="", durationStr=f"{ATR_LOOKBACK_DAYS} D",
                barSizeSetting=ATR_BAR_SIZE, whatToShow="TRADES", useRTH=False, formatDate=2,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if len(bars) < args.atr_period + 1:
                print(f"[{args.mode}] {symbol}: not enough recent bars to compute ATR")
                sys.exit(1)
            df = pd.DataFrame({
                "high": [b.high for b in bars], "low": [b.low for b in bars], "close": [b.close for b in bars],
            })
            atr = _atr(df, args.atr_period)
            trail_amount = round(atr * args.atr_multiplier, 2)
            if trail_amount <= 0:
                print(f"[{args.mode}] {symbol}: computed trail amount is not positive (ATR={atr})")
                sys.exit(1)
            order = Order(action=action, totalQuantity=args.qty, orderType="TRAIL", auxPrice=trail_amount, transmit=True)
            trade = ib.placeOrder(qualified, order)
            ib.sleep(1)
            print(f"[{args.mode}] {symbol}: atr_trailing_stop added: {args.qty} @ trail ${trail_amount:.2f} "
                  f"(ATR({args.atr_period})={atr:.4f}) (order_id={trade.order.orderId})")
        else:
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
