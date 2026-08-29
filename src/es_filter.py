"""ES (E-mini S&P 500 futures) VWAP directional market filter - shared by
cycle.py's live/paper entry gates and backtest_engine.py's backtest
tagging/before-after reporting.

Market direction is BULLISH when ES trades above its own session VWAP,
BEARISH when below (compute_market_direction) - a long setup is only
allowed while BULLISH, a short only while BEARISH (check()). Both halves
are pure/synchronous-data-in, no IBKR dependency of their own; only
fetch_live_direction talks to a live Gateway, kept as a thin wrapper so
the actual decision logic (check()) is fully unit-testable without one.

Fails OPEN: whenever ES's own direction can't be determined (no market
data entitlement, a disconnected Gateway, insufficient bars), check()
allows the trade through with reason "es_data_unavailable" rather than
silently blocking every strategy the instant one third-party feed
hiccups. Fail-closed was considered and explicitly rejected for this
reason - the caller is expected to surface the degraded state itself
(a rate-limited warning notify(), not a silent pass), since this module
has no notification channel of its own.
"""
from __future__ import annotations

import pandas as pd

from src import orb

ES_SYMBOL = "ES"
ES_EXCHANGE = "CME"
ES_BAR_SIZE = "5 mins"


def compute_market_direction(es_bars_today: pd.DataFrame | None) -> dict | None:
    """`es_bars_today` must already be sliced to a single session (same
    "resets daily" convention as orb._compute_vwap_series, which this
    reuses directly rather than duplicating the VWAP math). Returns
    {"es_price", "es_vwap", "direction": "BULLISH"|"BEARISH"} off the
    LAST bar in the slice, or None if there's no bar yet (too early in
    the session) or VWAP can't be computed (zero cumulative volume)."""
    if es_bars_today is None or es_bars_today.empty:
        return None
    vwap_series = orb._compute_vwap_series(es_bars_today)
    es_vwap = vwap_series.iloc[-1] if not vwap_series.empty else None
    if es_vwap is None or pd.isna(es_vwap):
        return None
    es_price = float(es_bars_today["Close"].iloc[-1])
    return {
        "es_price": es_price, "es_vwap": float(es_vwap),
        "direction": "BULLISH" if es_price > es_vwap else "BEARISH",
    }


def check(direction: dict | None, side: str) -> dict:
    """Pure gate decision. `side` is the STRATEGY's own trade side
    ("long"/"short"), not a fade strategy's signal_side - the filter
    cares what the bot is about to DO, matching the spec's "Market
    first, setup second" framing, not why it decided to do it.

    direction=None (see module docstring) -> allowed=True, reason
    "es_data_unavailable". Otherwise allowed only when ES's direction
    agrees with `side` - reason is exactly the spec's own rejection text
    ("ES Below VWAP" / "ES Above VWAP") so a caller can log/notify it
    verbatim."""
    if direction is None:
        return {"allowed": True, "reason": "es_data_unavailable", "direction": None,
                "es_price": None, "es_vwap": None}
    bullish = direction["direction"] == "BULLISH"
    if side == "long":
        allowed = bullish
        reason = "es_ok" if allowed else "ES Below VWAP"
    else:
        allowed = not bullish
        reason = "es_ok" if allowed else "ES Above VWAP"
    return {
        "allowed": allowed, "reason": reason, "direction": direction["direction"],
        "es_price": direction["es_price"], "es_vwap": direction["es_vwap"],
    }


def fetch_live_direction(ib) -> dict | None:
    """Live IBKR fetch for cycle.py's own entry gates, called once per
    scan (never per-symbol - see cycle.py's own call sites) - a fresh ES
    ContFuture (IBKR's own continuous-front-month contract, so this
    never has to track quarterly ES roll dates itself) qualified and
    queried for today's 5-minute bars, same bar size/whatToShow as every
    other intraday fetch in this codebase. Never raises - any failure
    (no futures market-data entitlement, a disconnected Gateway, an
    empty response) comes back as None, which check() above already
    treats as fail-open."""
    from ib_async import ContFuture

    try:
        contract = ContFuture(ES_SYMBOL, ES_EXCHANGE)
        qualified = ib.qualifyContracts(contract)
        if not qualified or qualified[0] is None:
            return None
        bars = ib.reqHistoricalData(
            qualified[0], endDateTime="", durationStr="1 D",
            barSizeSetting=ES_BAR_SIZE, whatToShow="TRADES", useRTH=False, formatDate=2,
            timeout=30,
        )
        if not bars:
            return None
        df = pd.DataFrame({
            "High": [float(b.high) for b in bars], "Low": [float(b.low) for b in bars],
            "Close": [float(b.close) for b in bars], "Volume": [float(b.volume) for b in bars],
        }, index=pd.DatetimeIndex([pd.Timestamp(b.date) for b in bars]))
        if df.empty:
            return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        # A "1 D" duration request can still include a prior session's
        # trailing bars near the open depending on Gateway/session-
        # boundary rounding - keep only today's own bars, matching
        # compute_market_direction's "already sliced to one session"
        # requirement.
        today = df.index.max().date()
        df = df[df.index.date == today]
        return compute_market_direction(df)
    except Exception:
        return None
