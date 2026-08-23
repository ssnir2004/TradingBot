"""Fetches/refreshes the local historical-bars cache backtest_engine.py
reads from (see src/backtest_data.py) — pulls 5-minute bars (including
premarket/after-hours, via useRTH=False) per symbol from IBKR's own
reqHistoricalData, which — unlike yfinance's ~60-day intraday cap — happily
returns 6 months of 5-minute bars in a single request (confirmed: ~21.7k
bars in under a minute). A single request per symbol also keeps total
request volume low, which matters more than it sounds: IBKR's paper
Gateway will drop the whole connection (not just reject one request) if
hit with too many historical-data requests in a short window - this bit
us once already from an earlier version of this script that split each
symbol's fetch into many small requests. Always incremental: a symbol
already cached is only topped up with the gap since its last cached bar,
so running this again later (see run_service.py's weekly schedule) just
extends the cache forward, never re-fetches what's already there.

Needs a live IB Gateway connection — connects with its own dedicated
client ID (IBKR_BACKTEST_CLIENT_ID), same pattern as trade.py — so run
this on the deployed server, not a network-locked dev sandbox. See
DEPLOY.md for the one-time initial backfill command.
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import dotenv_values
from ib_async import Stock

from src import db, mode_config
from src.backtest_data import load_cached_bars, merge_bars, save_cached_bars
from src.ibkr_client import IBKRClient
from src.notify import notify
from src.sp500_tickers import SP500_TICKERS

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")
BAR_SIZE = "5 mins"
DEFAULT_INITIAL_DURATION = "6 M"  # total depth for a symbol with no cache yet
MAX_GAP_FILL_DAYS = 180  # cap a single incremental fetch even if the cache is very stale
REQUEST_PAUSE_SECONDS = 2.0  # stay well under IBKR's historical-data pacing limits
REQUEST_TIMEOUT_SECONDS = 180  # a 6-month request alone took ~60s in testing; leave headroom


def _bars_to_df(bars) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame({
        "Open": [float(b.open) for b in bars],
        "High": [float(b.high) for b in bars],
        "Low": [float(b.low) for b in bars],
        "Close": [float(b.close) for b in bars],
        "Volume": [float(b.volume) for b in bars],
    }, index=pd.DatetimeIndex([pd.Timestamp(b.date) for b in bars]))
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(ET)
    else:
        df.index = df.index.tz_convert(ET)
    return df


def fetch_symbol(ib, symbol: str, initial_duration: str) -> dict:
    contract = Stock(symbol, "SMART", "USD")
    (qualified,) = ib.qualifyContracts(contract)
    existing = load_cached_bars(symbol, BAR_SIZE)

    duration = initial_duration
    if existing is not None and not existing.empty:
        last_cached = existing.index.max()
        gap_days = (datetime.now(ET) - last_cached).days
        if gap_days < 1:
            return {"symbol": symbol, "status": "up_to_date", "new_bars": 0, "total_bars": len(existing)}
        duration = f"{min(gap_days + 2, MAX_GAP_FILL_DAYS)} D"

    bars = ib.reqHistoricalData(
        qualified, endDateTime="", durationStr=duration,
        barSizeSetting=BAR_SIZE, whatToShow="TRADES", useRTH=False, formatDate=2,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    df = _bars_to_df(bars)
    if df.empty:
        return {"symbol": symbol, "status": "no_data", "new_bars": 0}

    merged = merge_bars(existing, df)
    save_cached_bars(symbol, BAR_SIZE, merged)
    new_count = len(merged) - (len(existing) if existing is not None else 0)
    return {"symbol": symbol, "status": "ok", "new_bars": new_count, "total_bars": len(merged)}


def run_fetch(account_id: int, symbols: list[str], duration: str = DEFAULT_INITIAL_DURATION) -> dict:
    """The actual fetch-everything routine, connecting to IBKR with its own
    dedicated client ID (never collides with the cycle's own connection or
    trade.py's) and disconnecting when done — importable so
    run_service.py's scheduler can call it directly on a weekly cadence,
    same pattern as morning_prefilter.run_scan/build_custom_universe.build_universe."""
    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, "paper"),
        int(env.get("IBKR_BACKTEST_CLIENT_ID", 4)),
    )
    # ib.RequestTimeout wraps EVERY blocking call on this client (including
    # qualifyContracts, not just reqHistoricalData) with an outer wait_for -
    # it must be at least REQUEST_TIMEOUT_SECONDS or it would cut off a
    # large reqHistoricalData request before that request's own (larger)
    # internal timeout ever gets a chance to apply. Without either timeout,
    # a stuck request hangs forever if the Gateway stops responding
    # mid-request; with it, fetch_symbol's caller already catches the
    # resulting error and logs it so the run moves on to the next symbol.
    # Do NOT switch to delayed data (reqMarketDataType(3)) here - it was
    # tried as a fix for an unrelated hang and turned out to silently break
    # reqHistoricalData for this account, which already has live data
    # entitlements: confirmed by isolated testing that the exact same
    # request succeeds without it.
    ibkr.ib.RequestTimeout = REQUEST_TIMEOUT_SECONDS + 20
    results = []
    try:
        for i, symbol in enumerate(symbols, start=1):
            # Printed (and flushed) per symbol, not just in the final
            # summary - this is the only way to tell "still working,
            # 200/500 done" apart from "stuck" when watching the run live,
            # since IBKR historical-data calls have no built-in timeout of
            # their own and can hang if the Gateway stops responding.
            print(f"[{i}/{len(symbols)}] {symbol} ...", end=" ", flush=True)
            try:
                result = fetch_symbol(ibkr.ib, symbol, duration)
                print(result["status"], flush=True)
            except Exception as exc:  # noqa: BLE001 - one bad symbol must not kill the run
                result = {"symbol": symbol, "status": "error", "error": str(exc)}
                print(f"error: {exc}", flush=True)
            results.append(result)
            time.sleep(REQUEST_PAUSE_SECONDS)
    finally:
        ibkr.disconnect()

    ok = sum(1 for r in results if r["status"] == "ok")
    up_to_date = sum(1 for r in results if r["status"] == "up_to_date")
    errors = [r for r in results if r["status"] == "error"]
    summary = {"total": len(results), "ok": ok, "up_to_date": up_to_date, "errors": len(errors), "results": results}
    notify(
        "Backtest data fetch",
        f"{ok} updated, {up_to_date} already current, {len(errors)} errors, {len(results)} total",
        "default",
    )
    return summary


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted.")
    parser.add_argument("--symbols", nargs="*", default=None, help="Defaults to the full S&P 500 universe")
    parser.add_argument("--duration", default=DEFAULT_INITIAL_DURATION,
                         help="Initial backfill depth for a symbol with no cache yet, e.g. '6 M'")
    parser.add_argument("--limit", type=int, default=None, help="Cap symbols fetched (testing only)")
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    symbols = args.symbols or list(SP500_TICKERS)
    if args.limit:
        symbols = symbols[: args.limit]

    summary = run_fetch(account_id, symbols, args.duration)
    print({k: v for k, v in summary.items() if k != "results"})
    if summary["errors"]:
        print("errors (first 10):", [r for r in summary["results"] if r["status"] == "error"][:10])
    sys.exit(0 if summary["ok"] + summary["up_to_date"] > 0 or not summary["errors"] else 1)


if __name__ == "__main__":
    main()
