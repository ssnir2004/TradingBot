"""Sanity-checks the local backtest bar cache (data/backtest_bars/) after
a fetch_backtest_data.py run — confirms every symbol in the current S&P
500 list actually has cached bars, that each one's cached depth roughly
matches what was asked for, and flags anything thin/stale/missing so a
bad or partial backfill doesn't go unnoticed until a backtest silently
comes up short on history. Read-only, no IBKR connection needed (same
reason src/backtest_data.py itself has none) — safe to run any time,
right after a fetch_backtest_data.py run or on its own.
"""
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from src.backtest_data import cache_coverage, cached_symbols
from src.sp500_tickers import SP500_TICKERS

ET = ZoneInfo("America/New_York")
BAR_SIZE = "5 mins"
# A symbol not topped up in this long (run_service.py's weekly schedule
# should keep every symbol within a few days of "now") flags a fetch that
# silently stopped reaching it, not just normal incremental staleness.
STALE_DAYS_FLOOR = 5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-days", type=int, default=200,
                         help="Expected minimum days of history each symbol should have cached.")
    parser.add_argument("--out", type=str, default=None,
                         help="Write every missing/thin symbol (one per line) to this file - "
                              "for a targeted re-fetch of just what still needs it, e.g. "
                              "fetch_backtest_data.py --symbols $(cat that-file).")
    args = parser.parse_args()

    expected = set(SP500_TICKERS)
    present = set(cached_symbols(BAR_SIZE))
    missing = sorted(expected - present)
    extra = sorted(present - expected)

    now = datetime.now(ET)
    thin = []
    stale = []
    ok = 0
    for symbol in sorted(expected & present):
        cov = cache_coverage(symbol, BAR_SIZE)
        if cov is None:
            missing.append(symbol)
            continue
        span_days = (cov["to"] - cov["from"]).days
        age_days = (now - cov["to"]).days
        # Bar count per day isn't a reliable signal on its own - a less
        # actively-traded symbol legitimately gets fewer 5-min bars during
        # premarket/after-hours (IBKR only returns a bar where a trade
        # actually happened), so it varies a lot by symbol even when the
        # fetch is completely fine. Span coverage - did the cache actually
        # reach back min_days - is the real "did the fetch finish" signal.
        if span_days < args.min_days - 5:
            thin.append((symbol, span_days, cov["bar_count"]))
        elif age_days > STALE_DAYS_FLOOR:
            stale.append((symbol, cov["to"].date(), age_days))
        else:
            ok += 1
    missing.sort()

    print(f"Expected symbols (current S&P 500 list): {len(expected)}")
    print(f"Cached symbols found on disk:            {len(present)}")
    print(f"OK (>= {args.min_days}d, fresh):          {ok}")
    print(f"Missing entirely:                        {len(missing)}")
    if missing:
        print("  " + ", ".join(missing))
    print(f"Thin (span short of {args.min_days}d):     {len(thin)}")
    for symbol, span_days, bar_count in thin[:20]:
        print(f"  {symbol}: {span_days}d span, {bar_count} bars")
    if len(thin) > 20:
        print(f"  ... and {len(thin) - 20} more")
    print(f"Stale (no update in > {STALE_DAYS_FLOOR}d): {len(stale)}")
    for symbol, last_date, age in stale[:20]:
        print(f"  {symbol}: last bar {last_date} ({age}d old)")
    if len(stale) > 20:
        print(f"  ... and {len(stale) - 20} more")
    if extra:
        shown = ", ".join(extra[:20]) + (" ..." if len(extra) > 20 else "")
        print(f"Cached but not in the current S&P 500 list ({len(extra)}, informational only): {shown}")

    if args.out:
        needs_fetch = sorted(missing) + sorted(s for s, _, _ in thin)
        with open(args.out, "w") as f:
            f.write("\n".join(needs_fetch) + ("\n" if needs_fetch else ""))
        print(f"Wrote {len(needs_fetch)} symbol(s) needing a fetch to {args.out}")


if __name__ == "__main__":
    main()
