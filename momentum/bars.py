"""Intraday + daily bars for the momentum suite.

Phase 1 (alert-only) deliberately uses **yfinance**, exactly like the live
S&P engine's own intraday path (cycle._get_5min_bars / cycle.get_chart_bars)
- no IBKR connection, so momentum-scan.service is fully independent of the
trading Gateway and can never collide with cycle.py's client id or perturb
its connection. A 1-3 minute data delay is acceptable for a *scan + alert*
whose detectors only ever act on closed 5-minute candles; phase 3 (live
execution) will add an IBKR reqRealTimeBars path for entry timing.

Every intraday pull for a candidate is also appended to a per-symbol pickle
under data/momentum_bars/ so a "poor backtest" can later replay real,
pre-screened data (the screener is live-only - it has no history).
"""
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from momentum import store

log = logging.getLogger("momentum.bars")

ET = ZoneInfo("America/New_York")
PROJECT_DIR = Path(__file__).resolve().parent.parent
BARS_DIR = PROJECT_DIR / "data" / "momentum_bars"

# yfinance interval string per our timeframe key, and how far back to pull.
# 1m is capped at 7d by yfinance; 5m at 60d. We only ever want *today*.
_YF = {
    "1m": ("1m", "2d"),
    "5m": ("5m", "5d"),
    "1d": ("1d", "6mo"),
}


def _path(symbol: str, timeframe: str) -> Path:
    return BARS_DIR / f"{symbol.replace(' ', '_')}__{timeframe}.pkl"


def _load_cache(symbol: str, timeframe: str) -> pd.DataFrame | None:
    p = _path(symbol, timeframe)
    if not p.exists():
        return None
    try:
        df = pd.read_pickle(p)
        return df if not df.empty else None
    except Exception:
        return None


def _save_cache(symbol: str, timeframe: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    BARS_DIR.mkdir(parents=True, exist_ok=True)
    existing = _load_cache(symbol, timeframe)
    if existing is not None:
        df = pd.concat([existing, df])
        df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_pickle(_path(symbol, timeframe))
    store.upsert_bars_meta(
        symbol, timeframe,
        df.index.min().isoformat(), df.index.max().isoformat(), len(df),
    )


def _download(symbol: str, timeframe: str) -> pd.DataFrame | None:
    interval, period = _YF[timeframe]
    try:
        df = yf.Ticker(symbol.replace(" ", "-")).history(
            period=period, interval=interval, prepost=(timeframe != "1d"),
        )
    except Exception as exc:
        log.warning("yfinance %s %s failed: %s", symbol, timeframe, exc)
        return None
    if df is None or df.empty:
        return None
    # normalise: tz-aware ET index, standard column names
    try:
        df.index = df.index.tz_convert(ET)
    except (TypeError, AttributeError):
        df.index = df.index.tz_localize("UTC").tz_convert(ET)
    df = df.rename(columns={c: c.capitalize() for c in df.columns})
    return df[["Open", "High", "Low", "Close", "Volume"]].dropna()


def get_intraday(symbol: str, timeframe: str = "5m", archive: bool = True) -> pd.DataFrame | None:
    """Fresh intraday bars for `symbol`. When archive=True (the default in
    the scan loop) the pull is merged into the on-disk cache for later
    backtesting."""
    df = _download(symbol, timeframe)
    if df is None:
        return _load_cache(symbol, timeframe)  # fall back to whatever we have
    if archive:
        _save_cache(symbol, timeframe, df)
    return df


def session_bars(df: pd.DataFrame, day=None) -> pd.DataFrame:
    """Rows belonging to one ET trading day (default: the last day present)."""
    if df is None or df.empty:
        return df
    d = day or df.index[-1].date()
    return df[df.index.map(lambda ts: ts.date() == d)]


def get_daily(symbol: str) -> pd.DataFrame | None:
    """Daily bars (for the G6 overhead-resistance check). Cached daily."""
    cached = _load_cache(symbol, "1d")
    if cached is not None and cached.index.max().date() >= pd.Timestamp.now(tz=ET).date():
        return cached
    df = _download(symbol, "1d")
    if df is not None:
        _save_cache(symbol, "1d", df)
        return _load_cache(symbol, "1d")
    return cached
