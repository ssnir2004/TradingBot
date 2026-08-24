"""Places a manual LIMIT entry order with an ATR-based native IBKR trailing
stop attached as a bracket child, spawned as a subprocess by the dashboard
(web/app.py's POST /api/positions/open). Runs on its own IBKR client ID
(IBKR_MANUAL_ENTRY_CLIENT_ID) so it never collides with any other
connection.

This is a plain broker action, not a bot-tracked position: the stop is a
real IBKR TRAIL order that IBKR itself adjusts as price moves favorably
(not periodically recomputed by us), and the resulting position is not
subject to force_close_et or any of the strategy engine's own position
management - once submitted, both orders live entirely in TWS/IBKR, same
as if they'd been placed there by hand. The ATR distance is computed once,
at submission time, from a fresh pull of recent 5-min bars (not the
separate backtest cache, which may be stale or missing this symbol).

Bracket mechanics: the entry (parent) is placed with transmit=False so it
sits un-submitted at IBKR until the trailing-stop (child, parentId set)
is placed with transmit=True right after - that second call transmits
both orders together as one atomic bracket. If the entry never fills, the
stop simply never activates (standard IBKR bracket behavior); nothing on
our end needs to poll for that.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values
from ib_async import Order, Stock

from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
ATR_BAR_SIZE = "5 mins"
ATR_LOOKBACK_DAYS = 10  # comfortably more than enough 5-min bars for a 14-period ATR
# Must stay well under web/app.py's SUBPROCESS_TIMEOUT (40s), which bounds
# this whole script - a small 10-day request is normally fast (~15s for a
# much larger 1-month pull in prior testing), leaving real margin for
# connect + qualify + both order placements too.
REQUEST_TIMEOUT_SECONDS = 25


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
    parser.add_argument("--side", required=True, choices=["BUY", "SELL"])
    parser.add_argument("--qty", required=True, type=int)
    parser.add_argument("--limit-price", required=True, type=float)
    parser.add_argument("--atr-period", type=int, default=14)
    parser.add_argument("--atr-multiplier", required=True, type=float)
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    symbol = args.symbol.strip().upper()

    if args.qty <= 0:
        print("qty must be positive")
        sys.exit(1)
    if args.limit_price <= 0:
        print("limit_price must be positive")
        sys.exit(1)
    if args.atr_multiplier <= 0:
        print("atr_multiplier must be positive")
        sys.exit(1)

    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_MANUAL_ENTRY_CLIENT_ID", 16)),
    )
    try:
        ib = ibkr.ib
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)

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

        close_action = "SELL" if args.side == "BUY" else "BUY"

        parent = Order(
            action=args.side, totalQuantity=args.qty, orderType="LMT",
            lmtPrice=round(args.limit_price, 2), tif="DAY", transmit=False,
        )
        parent_trade = ib.placeOrder(qualified, parent)
        ib.sleep(0.5)

        stop = Order(
            action=close_action, totalQuantity=args.qty, orderType="TRAIL",
            auxPrice=trail_amount, parentId=parent_trade.order.orderId, transmit=True,
        )
        stop_trade = ib.placeOrder(qualified, stop)
        ib.sleep(0.5)

        print(f"[{args.mode}] {args.side} {args.qty} {symbol} @ LMT {args.limit_price:.2f} "
              f"(entry order_id={parent_trade.order.orderId}) — "
              f"ATR({args.atr_period})={atr:.4f}, trail=${trail_amount:.2f} "
              f"(stop order_id={stop_trade.order.orderId})")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
