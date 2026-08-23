"""Local cache of historical bars for the backtest engine, from either of
its two data sources: IBKR's own reqHistoricalData (5-minute intraday
bars, via fetch_backtest_data.py — unlike yfinance's ~60-day intraday
cap, IBKR can pull months per request) or yfinance (daily bars, fetched
directly by backtest_engine.py itself since no broker connection is
needed for those). Either way, once a bar is fetched it's cached here
FOREVER, so the usable backtest window only ever grows (bounded by how
far back has been fetched so far, not by any rolling data-provider
window). This module only knows how to read/merge/save what's already on
disk — no IBKR or yfinance dependency of its own — so it's safe to import
from the dashboard process (which never talks to IBKR directly) too.

One file per symbol+bar-size under data/backtest_bars/, holding every bar
ever fetched for that symbol, deduplicated and sorted by timestamp. The
bar-size string is just a cache key, not validated against any real
source — "5 mins" (IBKR intraday) and "1 day" (yfinance daily) coexist
as separate files for the same symbol.
"""
import pandas as pd
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
BARS_DIR = PROJECT_DIR / "data" / "backtest_bars"


def _cache_path(symbol: str, bar_size: str) -> Path:
    # "__" (double underscore) separates the two fields so a single-
    # underscore space-substitution (matching sp500_tickers.py's IBKR
    # contract format, e.g. "BRK B" -> "BRK_B") can be reversed
    # unambiguously when listing cached symbols back out.
    safe_symbol = symbol.replace(" ", "_")
    safe_bar_size = bar_size.replace(" ", "_")
    return BARS_DIR / f"{safe_symbol}__{safe_bar_size}.pkl"


def load_cached_bars(symbol: str, bar_size: str) -> pd.DataFrame | None:
    path = _cache_path(symbol, bar_size)
    if not path.exists():
        return None
    try:
        df = pd.read_pickle(path)
    except Exception:
        return None
    return df if not df.empty else None


def save_cached_bars(symbol: str, bar_size: str, bars: pd.DataFrame):
    BARS_DIR.mkdir(parents=True, exist_ok=True)
    bars = bars[~bars.index.duplicated(keep="last")].sort_index()
    bars.to_pickle(_cache_path(symbol, bar_size))


def merge_bars(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """Combines cached bars with freshly-fetched ones, deduplicated by
    timestamp (a fetch's small overlap with what's already cached is
    expected and harmless — the newer copy of an overlapping bar wins)."""
    if existing is None or existing.empty:
        return new.sort_index()
    if new.empty:
        return existing.sort_index()
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


def cached_symbols(bar_size: str) -> list[str]:
    """Every symbol with some cached data at this bar size — what the
    backtest engine can actually test a strategy against right now."""
    if not BARS_DIR.exists():
        return []
    suffix = f"__{bar_size.replace(' ', '_')}.pkl"
    return sorted(
        p.name[: -len(suffix)].replace("_", " ")
        for p in BARS_DIR.glob(f"*{suffix}")
    )


def cache_coverage(symbol: str, bar_size: str) -> dict | None:
    """{"from": ..., "to": ..., "bar_count": ...} for this symbol's cache,
    or None if nothing is cached yet — lets the backtest UI show what date
    range is actually available before someone picks one that's empty."""
    bars = load_cached_bars(symbol, bar_size)
    if bars is None:
        return None
    return {"from": bars.index.min(), "to": bars.index.max(), "bar_count": len(bars)}
