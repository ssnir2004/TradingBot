"""Builds/refreshes a custom stock universe cache (see
src/custom_universes.py) for strategies that restrict themselves to a
narrower, fundamentals-screened list than the default S&P 500 scan — e.g.
"ixic_large_beta_buy": NASDAQ-listed companies with market cap above a
threshold, beta above a threshold, and an analyst consensus of Buy or
better.

Needs real internet access (the NASDAQ-listed symbol directory, then one
yfinance fundamentals lookup per candidate) — run this on the deployed
server or any machine with normal internet, not inside a network-locked
sandbox. Unlike morning_prefilter.py's daily gap scan, this doesn't need to
run every morning: market cap/beta/analyst rating drift over weeks, not
minutes, so a weekly cron entry is plenty (see src/custom_universes.py's
max_staleness_days for how long a stale cache is still trusted).
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
import yfinance as yf

from src.custom_universes import CUSTOM_UNIVERSES, ET
from src.notify import notify
from src.sp500_tickers import SP500_TICKERS

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
# CLI/API defaults - only ever used for a universe whose own spec (see
# CUSTOM_UNIVERSES) doesn't set default_min_market_cap/beta/recommendation_mean,
# which none currently do (every registered universe sets its own).
DEFAULT_MIN_MARKET_CAP = 1_000_000_000
DEFAULT_MIN_BETA = 1.2
DEFAULT_MIN_RECOMMENDATION_MEAN = 2.0  # yfinance scale: 1.0=Strong Buy ... 5.0=Strong Sell
ACCEPTABLE_RECOMMENDATION_KEYS = {"strong_buy", "buy"}
REQUEST_TIMEOUT = 15
REQUEST_STAGGER_SECONDS = 0.3
DEFAULT_WORKERS = 2


def _yahoo_to_ibkr(ticker: str) -> str:
    return ticker.replace(".", " ")


def _fetch_candidate_symbols(source: str) -> list[str]:
    """Yahoo-format candidate symbols for whichever source a universe's
    spec names - "nasdaq_directory" (the original behavior, a live scrape
    of every NASDAQ-listed common stock) or "sp500" (the project's own
    hardcoded S&P 500 list, converted from its IBKR format - space before
    a share class letter, e.g. "BRK B" - to Yahoo's dot form "BRK.B", the
    same convention _yahoo_to_ibkr already reverses on the way out)."""
    if source == "sp500":
        return sorted(t.replace(" ", ".") for t in SP500_TICKERS)
    return fetch_nasdaq_listed_symbols()


def fetch_nasdaq_listed_symbols() -> list[str]:
    """Downloads the public NASDAQ-listed symbol directory and returns
    common-stock tickers only (drops ETFs, test issues, and anything with
    stray characters that usually marks warrants/units/rights)."""
    resp = requests.get(NASDAQ_LISTED_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    lines = resp.text.strip().splitlines()
    header = lines[0].split("|")
    idx = {name: i for i, name in enumerate(header)}
    symbols = []
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            continue
        fields = line.split("|")
        if len(fields) != len(header):
            continue
        symbol = fields[idx["Symbol"]].strip()
        test_issue = fields[idx["Test Issue"]].strip()
        etf = fields[idx["ETF"]].strip()
        if not symbol or test_issue != "N" or etf != "N":
            continue
        if any(ch in symbol for ch in "$^="):
            continue
        symbols.append(symbol)
    return sorted(set(symbols))


def _fetch_info(symbol: str) -> dict | None:
    """One attempt at yf.Ticker(symbol).info, with a single retry after a
    short backoff - Yahoo's fundamentals endpoint (unlike the plain-OHLCV
    endpoint morning_prefilter.py uses, which needs no auth) requires a
    session crumb/cookie that occasionally gets rejected under load
    ("Invalid Crumb" / HTTP 401); one retry a couple seconds later clears
    most of those without having to re-run the whole scan."""
    for attempt in range(2):
        try:
            info = yf.Ticker(symbol).info
        except Exception:  # noqa: BLE001 - one bad ticker must not kill the scan
            info = None
        if info:
            return info
        if attempt == 0:
            time.sleep(2.0)
    return None


def _screen_one(
    symbol: str, min_market_cap: float, min_beta: float | None, min_recommendation_mean: float | None,
) -> dict | None:
    """min_beta/min_recommendation_mean of None skips that check entirely
    (unlike min_market_cap, which every universe requires) - used by the
    sp500_marketcap_1b universe, which only screens on market cap."""
    info = _fetch_info(symbol)
    if not info:
        return None

    market_cap = info.get("marketCap")
    beta = info.get("beta")
    recommendation_key = (info.get("recommendationKey") or "").lower()
    recommendation_mean = info.get("recommendationMean")

    if market_cap is None or market_cap < min_market_cap:
        return None
    if min_beta is not None and (beta is None or beta < min_beta):
        return None
    if min_recommendation_mean is not None:
        rating_ok = recommendation_key in ACCEPTABLE_RECOMMENDATION_KEYS or (
            recommendation_mean is not None and recommendation_mean <= min_recommendation_mean
        )
        if not rating_ok:
            return None

    return {
        "symbol": symbol,
        "market_cap": market_cap,
        "beta": beta,
        "recommendation_key": recommendation_key,
        "recommendation_mean": recommendation_mean,
    }


def build_universe(
    universe_key: str,
    min_market_cap: float | None = None,
    min_beta: float | None = None,
    min_recommendation_mean: float | None = None,
    limit: int | None = None,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
) -> dict:
    """min_market_cap/min_beta/min_recommendation_mean of None (the
    default) fall back to this universe's OWN default_min_* fields in
    CUSTOM_UNIVERSES - each universe screens on its own criteria (e.g.
    sp500_marketcap_1b has no beta/rating requirement at all) instead of
    every universe being forced through the same global thresholds, which
    is how run_service.py's weekly rebuild job now calls this for every
    registered universe uniformly (see its own comment)."""
    started = time.monotonic()
    spec = CUSTOM_UNIVERSES.get(universe_key)
    if spec is None:
        result = {"success": False, "error": f"Unknown universe {universe_key!r}; known: {sorted(CUSTOM_UNIVERSES)}"}
        print(json.dumps(result))
        return result

    if min_market_cap is None:
        min_market_cap = spec.get("default_min_market_cap", DEFAULT_MIN_MARKET_CAP)
    if min_beta is None:
        min_beta = spec.get("default_min_beta", DEFAULT_MIN_BETA)
    if min_recommendation_mean is None:
        min_recommendation_mean = spec.get("default_min_recommendation_mean", DEFAULT_MIN_RECOMMENDATION_MEAN)
    source = spec.get("symbol_source", "nasdaq_directory")

    try:
        candidates = _fetch_candidate_symbols(source)
    except requests.RequestException as exc:
        result = {"success": False, "error": f"Failed to fetch candidate symbols ({source}): {exc}"}
        print(json.dumps(result))
        notify(f"Universe build FAILED ({universe_key})", result["error"], "high")
        return result

    if limit:
        candidates = candidates[:limit]

    survivors = []
    failed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Submitted with a small stagger, not all at once - bursting
        # thousands of fundamentals requests at Yahoo in the same instant
        # (even through only a couple of worker threads) reads as scraping
        # and gets the session's crumb rejected wholesale ("Invalid Crumb"
        # on every single ticker), which happened the first time this
        # ran with 8 workers submitted in one burst. This is a slow,
        # deliberately gentle scan - it only needs to run once a week.
        futures = {}
        for symbol in candidates:
            futures[pool.submit(_screen_one, symbol, min_market_cap, min_beta, min_recommendation_mean)] = symbol
            time.sleep(REQUEST_STAGGER_SECONDS)
        for future in as_completed(futures):
            try:
                hit = future.result()
            except Exception:  # noqa: BLE001
                hit = None
            if hit is None:
                failed += 1
            else:
                survivors.append(hit)

    survivors.sort(key=lambda s: s["market_cap"], reverse=True)
    tickers = [_yahoo_to_ibkr(s["symbol"]) for s in survivors]
    elapsed = round(time.monotonic() - started, 2)
    now_iso = datetime.now(ET).isoformat(timespec="seconds")

    payload = {
        "universe": universe_key,
        "generated_at": now_iso,
        "params": {
            "min_market_cap": min_market_cap,
            "min_beta": min_beta,
            "min_recommendation_mean": min_recommendation_mean,
        },
        "total_candidates": len(candidates),
        "survivors_count": len(tickers),
        "tickers": tickers,
    }

    if not dry_run:
        spec["cache_path"].parent.mkdir(parents=True, exist_ok=True)
        spec["cache_path"].write_text(json.dumps(payload, indent=2))

    result = {
        "success": True,
        "universe": universe_key,
        "total_candidates": len(candidates),
        "survivors_count": len(tickers),
        "no_data_or_rejected": failed,
        "elapsed_seconds": elapsed,
        "cache_path": str(spec["cache_path"]),
        "sample": tickers[:20],
    }
    print(json.dumps(result))

    if not dry_run:
        notify(
            f"Universe build ({universe_key})",
            f"{len(tickers)}/{len(candidates)} survivors in {elapsed}s",
            "default",
        )

    return result


def main():
    parser = argparse.ArgumentParser()
    # These three default to None (not DEFAULT_MIN_*) - build_universe falls
    # back to the TARGET UNIVERSE's own default_min_* (see CUSTOM_UNIVERSES)
    # when unset, so leaving them off this command line means "whatever this
    # universe normally screens on" rather than always forcing every
    # universe through ixic_large_beta_buy's own numbers.
    parser.add_argument("--universe", default="ixic_large_beta_buy", choices=sorted(CUSTOM_UNIVERSES))
    parser.add_argument("--min-market-cap", type=float, default=None)
    parser.add_argument("--min-beta", type=float, default=None)
    parser.add_argument("--min-recommendation-mean", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Cap candidates screened (testing only)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = build_universe(
        args.universe, args.min_market_cap, args.min_beta, args.min_recommendation_mean,
        args.limit, args.workers, args.dry_run,
    )
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
