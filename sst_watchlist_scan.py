"""SST Swing's universe screen (source prompt section 6; see
docs/sst_swing_spec.md and the G-SST-4 gap decisions) - a weekly,
symbol-level scoring/filtering utility, entirely separate from the
per-trade signal logic in src/sst_swing.py. Writes db.sst_watchlist,
which sst_swing_live.py's daily entry scan (status='pass' only) and
web/templates/sst_watchlist.html (every row) both read.

Four scores per symbol, all computed from the same already-fetched daily
frame:
  - noise_score: mean (high-low) wick relative to the day's own body
    over noise_lookback_days - lower is "cleaner". Ranked only (G-SST-4:
    no hard cutoff, the source prompt itself only ever says "lower is
    better").
  - step_regularity_ratio: of the significant days (src.sst_swing.
    classify_days) in step_regularity_lookback_days, the fraction that
    extended in the SAME direction as the prevailing SMA50 trend -
    "how consistently daily closes trend without large single-bar
    reversals" per the source prompt. Ranked only.
  - correlation_spy: Pearson correlation of daily % returns against
    REFERENCE_SYMBOL over correlation_lookback_days. correlation_pass_max
    (0.3) or below -> 'pass'; up to correlation_reject_min (0.5) ->
    'review' (source prompt: "target range roughly 0.05-0.3... flag
    anything above ~0.5" - G-SST-4 turned that into an explicit pass/
    review split rather than one hard cutoff, since the source itself
    only commits to the top end); at or above reject_min, the symbol is
    dropped entirely - no row.
  - avg_dollar_volume / price: liquidity_min_avg_dollar_volume and
    price_floor_usd are HARD floors, checked first (cheapest checks,
    and every symbol below either is excluded regardless of how good its
    other scores are) - a symbol failing either never gets a row, and
    correlation is never even computed for it.

Needs real internet access (yfinance) - run on the deployed server, not
a locked-down sandbox, same caveat as build_custom_universe.py. Weekly
cadence (market structure/correlation drift over weeks, not days) - see
run_service.py's own scheduling comment for this job.

Memory-safe by construction: fetches SP500_TICKERS in small chunks (see
momentum/backfill.py's stream_daily_candidates, the precedent this
mirrors) rather than one giant multi-ticker yf.download - this server
runs the live trading engine on well under 1GB RAM.
"""
import argparse
import gc
import sys
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from src import db
from src.custom_universes import ET
from src.notify import notify
from src.sp500_tickers import SP500_TICKERS
from src.sst_swing import classify_days

CHUNK_SIZE = 25
FETCH_PERIOD = "6mo"  # comfortably covers every lookback below (max 60 trading days) plus SMA50's own warmup
REFERENCE_SYMBOL = "SPY"

NOISE_LOOKBACK_DAYS = 60
STEP_REGULARITY_LOOKBACK_DAYS = 60
CORRELATION_LOOKBACK_DAYS = 60
SMA_PERIOD = 50  # same trend definition src.sst_swing.evaluate_sst_entry itself uses

CORRELATION_PASS_MAX = 0.3
CORRELATION_REJECT_MIN = 0.5
LIQUIDITY_MIN_AVG_DOLLAR_VOLUME = 5_000_000.0
PRICE_FLOOR_USD = 10.0


def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _yahoo_to_ibkr(ticker: str) -> str:
    return ticker.replace(".", " ")


def compute_noise_score(daily: pd.DataFrame, lookback: int = NOISE_LOOKBACK_DAYS) -> float | None:
    window = daily.tail(lookback)
    if len(window) < max(5, lookback // 4):
        return None
    # A near-doji day's body is ~0 - floored so it can't blow up the
    # average with a single absurd ratio.
    body = (window["Close"] - window["Open"]).abs().clip(lower=0.01)
    wick = window["High"] - window["Low"]
    return float((wick / body).mean())


def compute_step_regularity(daily: pd.DataFrame, lookback: int = STEP_REGULARITY_LOOKBACK_DAYS,
                            sma_period: int = SMA_PERIOD) -> float | None:
    if len(daily) < sma_period + 5:
        return None
    sma = daily["Close"].rolling(sma_period).mean()
    last_sma = sma.iloc[-1]
    if pd.isna(last_sma):
        return None
    trend_up = float(daily["Close"].iloc[-1]) > float(last_sma)

    days = classify_days(daily)
    sig = days[days["is_significant"]].tail(lookback)
    if len(sig) == 0:
        return None
    prior_high = daily["High"].shift(1).reindex(sig.index)
    prior_low = daily["Low"].shift(1).reindex(sig.index)
    extends_up = sig["High"] > prior_high
    extends_down = sig["Low"] < prior_low
    in_trend = extends_up if trend_up else extends_down
    return float(in_trend.sum() / len(sig))


def compute_correlation(daily: pd.DataFrame, ref_daily: pd.DataFrame,
                        lookback: int = CORRELATION_LOOKBACK_DAYS) -> float | None:
    a = daily["Close"].pct_change().tail(lookback)
    b = ref_daily["Close"].pct_change().tail(lookback)
    joined = pd.concat([a, b], axis=1, join="inner").dropna()
    if len(joined) < max(10, lookback // 3):
        return None
    corr = joined.iloc[:, 0].corr(joined.iloc[:, 1])
    return float(corr) if pd.notna(corr) else None


def screen_one(symbol: str, daily: pd.DataFrame, ref_daily: pd.DataFrame, params: dict) -> dict | None:
    """None = excluded entirely (a hard floor, or the correlation
    reject band) - never gets a row in sst_watchlist at all."""
    if daily is None or len(daily) < 60:
        return None
    price = float(daily["Close"].iloc[-1])
    if price < params["price_floor_usd"]:
        return None
    avg_dollar_volume = float((daily["Close"] * daily["Volume"]).tail(20).mean())
    if pd.isna(avg_dollar_volume) or avg_dollar_volume < params["liquidity_min_avg_dollar_volume"]:
        return None

    corr = compute_correlation(daily, ref_daily, params["correlation_lookback_days"])
    if corr is not None and abs(corr) >= params["correlation_reject_min"]:
        return None
    status = "pass" if corr is not None and abs(corr) <= params["correlation_pass_max"] else "review"

    return {
        "symbol": _yahoo_to_ibkr(symbol),
        "status": status,
        "noise_score": compute_noise_score(daily, params["noise_lookback_days"]),
        "correlation_spy": corr,
        "step_regularity_ratio": compute_step_regularity(daily, params["step_regularity_lookback_days"], params["sma_period"]),
        "avg_dollar_volume": avg_dollar_volume,
        "price": price,
    }


def run_scan(symbols: list[str] | None = None, params: dict | None = None, dry_run: bool = False) -> dict:
    started = time.monotonic()
    symbols = symbols if symbols is not None else sorted(SP500_TICKERS)
    p = {
        "noise_lookback_days": NOISE_LOOKBACK_DAYS,
        "step_regularity_lookback_days": STEP_REGULARITY_LOOKBACK_DAYS,
        "correlation_lookback_days": CORRELATION_LOOKBACK_DAYS,
        "sma_period": SMA_PERIOD,
        "correlation_pass_max": CORRELATION_PASS_MAX,
        "correlation_reject_min": CORRELATION_REJECT_MIN,
        "liquidity_min_avg_dollar_volume": LIQUIDITY_MIN_AVG_DOLLAR_VOLUME,
        "price_floor_usd": PRICE_FLOOR_USD,
    }
    if params:
        p.update(params)

    ref = yf.download(tickers=REFERENCE_SYMBOL, period=FETCH_PERIOD, interval="1d",
                      threads=False, progress=False, auto_adjust=False)
    if ref is None or ref.empty:
        result = {"success": False, "error": f"failed to fetch reference symbol {REFERENCE_SYMBOL}"}
        print(result)
        notify("SST watchlist scan FAILED", result["error"], "high")
        return result
    if isinstance(ref.columns, pd.MultiIndex):
        ref = ref.xs(REFERENCE_SYMBOL, axis=1, level=1)

    rows = []
    failed = 0
    chunks = list(_chunked(symbols, CHUNK_SIZE))
    for i, chunk in enumerate(chunks):
        try:
            df = yf.download(tickers=chunk, period=FETCH_PERIOD, interval="1d",
                             group_by="ticker", threads=False, progress=False, auto_adjust=False)
        except Exception:  # noqa: BLE001 - one bad chunk must not kill the whole weekly scan
            failed += len(chunk)
            continue
        for sym in chunk:
            try:
                sub = df[sym] if len(chunk) > 1 else df
            except (KeyError, TypeError):
                failed += 1
                continue
            sub = sub.dropna(how="all")
            hit = screen_one(sym, sub, ref, p)
            if hit is not None:
                rows.append(hit)
            else:
                failed += 1
        del df
        if i % 5 == 0:
            gc.collect()

    rows.sort(key=lambda r: (r["noise_score"] if r["noise_score"] is not None else float("inf")))

    elapsed = round(time.monotonic() - started, 2)
    if not dry_run:
        db.replace_sst_watchlist(rows)

    result = {
        "success": True,
        "total_candidates": len(symbols),
        "survivors_count": len(rows),
        "pass_count": sum(1 for r in rows if r["status"] == "pass"),
        "review_count": sum(1 for r in rows if r["status"] == "review"),
        "excluded_count": failed,
        "elapsed_seconds": elapsed,
        "sample": [r["symbol"] for r in rows[:20]],
    }
    print(result)
    if not dry_run:
        notify(
            "SST watchlist scan",
            f"{result['survivors_count']}/{result['total_candidates']} survivors "
            f"({result['pass_count']} pass, {result['review_count']} review) in {elapsed}s",
            "default",
        )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Cap symbols screened (testing only)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    symbols = sorted(SP500_TICKERS)
    if args.limit:
        symbols = symbols[:args.limit]
    result = run_scan(symbols, dry_run=args.dry_run)
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    from pathlib import Path
    from src import db as _db
    _db.init_db(seed_rules_path=Path(__file__).resolve().parent / "rules.json")
    main()
