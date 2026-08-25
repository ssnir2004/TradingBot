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
# ~78 5-min bars per regular session (6.5h) alone, well over that with
# useRTH=False (premarket 4:00-9:30 + regular + after-hours 16:00-20:00
# adds ~140 more bars/day) — anything drastically under this per cached
# day is a sign that symbol's fetch got cut short (e.g. hit an Error 162
# mid-chunk and only partially saved) rather than genuinely thin trading.
BARS_PER_DAY_FLOOR = 100
# A symbol not topped up in this long (run_service.py's weekly schedule
# should keep every symbol within a few days of "now") flags a fetch that
# silently stopped reaching it, not just normal incremental staleness.
STALE_DAYS_FLOOR = 5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-days", type=int, default=200,
                         help="Expected minimum days of history each symbol should have cached.")
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
        if span_days < args.min_days - 5 or cov["bar_count"] < span_days * BARS_PER_DAY_FLOOR:
            thin.append((symbol, span_days, cov["bar_count"]))
        elif age_days > STALE_DAYS_FLOOR:
            stale.append((symbol, cov["to"].date(), age_days))
        else:
            ok += 1
    missing.sort()

    print(f"Expected symbols (current S&P 500 list): {len(expected)}")
    print(f"Cached symbols found on disk:            {len(present)}")
    print(f"OK (>= {args.min_days}d, dense, fresh):  {ok}")
    print(f"Missing entirely:                        {len(missing)}")
    if missing:
        print("  " + ", ".join(missing))
    print(f"Thin (short span or too few bars/day):   {len(thin)}")
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


if __name__ == "__main__":
    main()
