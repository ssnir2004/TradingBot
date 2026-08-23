"""Fetches/refreshes the local historical-bars cache backtest_engine.py
reads from (see src/backtest_data.py) — pulls 5-minute bars (including
premarket/after-hours, via useRTH=False) per symbol from IBKR's own
reqHistoricalData, which — unlike yfinance's ~60-day intraday cap — can
go back years, but NOT in a single request: IBKR happily returns 6
months of 5-minute bars in one call (confirmed: ~21.7k bars in under a
minute), but a single request for 1 year+ is reliably cancelled by
IBKR's own historical-data service after ~5 minutes regardless of how
generous a client-side timeout is given (confirmed via reqHeadTimeStamp
+ live testing: 1 Y, 3 Y, and 10 Y single-shot requests for AAPL all
failed with "Error 162: API historical data query cancelled", even
though reqHeadTimeStamp claims data exists back to 1980 - that claim
reflects raw tick data existing, not that 5-minute bars are
reconstructible from it in one server-side call). So a symbol's
first-ever backfill is paged backward in CHUNK_DAYS-sized (~6 month)
chunks rather than asked for in one request. Keep chunks at this size,
not smaller: many small requests is what caused the Gateway to drop the
whole connection once already (IBKR's paper Gateway punishes request
count over a short window, not request size) - few large chunks avoids
that while still respecting the ~6-month per-request ceiling. Always
incremental: a symbol already cached is only topped up with the gap
since its last cached bar (never re-backfills further into the past for
a symbol that already has some cache), so running this again later (see
run_service.py's weekly schedule) just extends the cache forward, never
re-fetches what's already there.

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
DEFAULT_INITIAL_DURATION = "2 Y"  # total depth for a symbol with no cache yet
CHUNK_DAYS = 180  # the largest single reqHistoricalData span confirmed to work (~6 months)
MAX_GAP_FILL_DAYS = CHUNK_DAYS  # an incremental top-up is always a single request, so it
# must stay within the same per-request ceiling as one initial-backfill chunk
REQUEST_PAUSE_SECONDS = 2.0  # stay well under IBKR's historical-data pacing limits
REQUEST_TIMEOUT_SECONDS = 180  # a 6-month request alone took ~60s in testing; leave headroom
# ib.connect() returns as soon as the socket is up, but its own internal
# account-state sync (positions/open orders/account updates/executions) can
# still be catching up in the background for a few seconds after that -
# confirmed via repeated live runs: the very first reqHistoricalData call,
# fired immediately after connect(), reliably fails while later ones (same
# connection, seconds later) succeed. A short pause here before the first
# real request is cheaper than losing symbol 1 on every run.
CONNECT_WARMUP_SECONDS = 5
# IBKR's documented HARD pacing cap ("no more than 60 historical-data
# requests per rolling 10 minutes") is explicitly lifted for bar sizes of
# "1 min" and up (ours is "5 mins" - see interactivebrokers.github.io/
# tws-api/historical_limitations.html), so the repeated Error 162 /
# Timeout clusters seen in this account are NOT that fixed 60-per-10-min
# wall - they're IBKR's undocumented "soft" throttle that dynamically
# load-balances client requests against server load. That means: no fixed
# safe request count exists to design around, but the throttle is often
# transient and clears within seconds to low minutes (unlike a real
# 10-minute hard-limit reset) - so retrying a failed symbol a couple of
# times with a short backoff, right here, recovers most of what a full
# stop-the-script-and-wait-25-minutes cycle was fixing by brute force.
RETRY_ATTEMPTS = 3  # 1 initial try + 2 retries
RETRY_BACKOFF_SECONDS = [15, 45]

_DURATION_UNIT_DAYS = {"D": 1, "W": 7, "M": 30, "Y": 365}


def _duration_to_days(duration: str) -> int:
    amount, unit = duration.strip().split()
    return int(amount) * _DURATION_UNIT_DAYS[unit.upper()[0]]


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


def _fetch_span(ib, qualified, total_days: int) -> pd.DataFrame:
    """Pages backward from now in CHUNK_DAYS-sized reqHistoricalData calls
    until `total_days` of history is covered (see module docstring for why
    a single request can't just ask for the whole span directly). A
    total_days at or under CHUNK_DAYS - the common case, an incremental
    top-up - takes exactly one iteration, i.e. one request."""
    frames = []
    remaining = total_days
    cursor = ""  # "" means "now" for IBKR's endDateTime; a datetime after that
    while remaining > 0:
        step = min(remaining, CHUNK_DAYS)
        bars = ib.reqHistoricalData(
            qualified, endDateTime=cursor, durationStr=f"{step} D",
            barSizeSetting=BAR_SIZE, whatToShow="TRADES", useRTH=False, formatDate=2,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        df = _bars_to_df(bars)
        if df.empty:
            break
        frames.append(df)
        cursor = df.index.min().tz_convert("UTC").to_pydatetime()
        remaining -= step
        if remaining > 0:
            time.sleep(REQUEST_PAUSE_SECONDS)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames).sort_index()
    return combined[~combined.index.duplicated(keep="last")]


def fetch_symbol(ib, symbol: str, initial_duration: str) -> dict:
    contract = Stock(symbol, "SMART", "USD")
    (qualified,) = ib.qualifyContracts(contract)
    existing = load_cached_bars(symbol, BAR_SIZE)

    if existing is not None and not existing.empty:
        last_cached = existing.index.max()
        gap_days = (datetime.now(ET) - last_cached).days
        if gap_days < 1:
            return {"symbol": symbol, "status": "up_to_date", "new_bars": 0, "total_bars": len(existing)}
        total_days = min(gap_days + 2, MAX_GAP_FILL_DAYS)
    else:
        total_days = _duration_to_days(initial_duration)

    df = _fetch_span(ib, qualified, total_days)
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
    time.sleep(CONNECT_WARMUP_SECONDS)
    results = []
    try:
        for i, symbol in enumerate(symbols, start=1):
            # Printed (and flushed) per symbol, not just in the final
            # summary - this is the only way to tell "still working,
            # 200/500 done" apart from "stuck" when watching the run live,
            # since IBKR historical-data calls have no built-in timeout of
            # their own and can hang if the Gateway stops responding.
            print(f"[{i}/{len(symbols)}] {symbol} ...", end=" ", flush=True)
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    result = fetch_symbol(ibkr.ib, symbol, duration)
                except Exception as exc:  # noqa: BLE001 - one bad symbol must not kill the run
                    result = {"symbol": symbol, "status": "error", "error": f"{type(exc).__name__}: {exc}"}
                if result["status"] not in ("error", "no_data") or attempt == RETRY_ATTEMPTS - 1:
                    break
                print(f"{result['status']} (retry {attempt + 1}/{RETRY_ATTEMPTS - 1}) ...", end=" ", flush=True)
                time.sleep(RETRY_BACKOFF_SECONDS[attempt])
            if result["status"] == "error":
                print(f"error: {result['error']}", flush=True)
            else:
                print(result["status"], flush=True)
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
                         help="Initial backfill depth for a symbol with no cache yet, e.g. '2 Y'")
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
