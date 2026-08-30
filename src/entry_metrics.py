"""Point-in-time market/stock/setup context captured at an ORB trade's own
entry timestamp - pure enrichment for offline analysis (see
analyze_entry_metrics.py), never read by any strategy decision. Nothing
here influences entry/exit logic, position sizing, or risk in any way -
every value here is computed AFTER a trade has already been decided, off
data the strategy itself already had (or safely could have had) at that
exact moment.

STRICT point-in-time discipline: every function here takes bars ALREADY
SLICED to <= the entry timestamp by the caller (backtest_engine.py) - see
each function's own docstring for which slice it expects. Daily-level
indicators (EMA20/50, 52-week high, gap, previous day high/low) read off
`daily`, which by this module's own contract must already END at the
prior completed trading day (same convention orb.py/cycle.py's own D1-D3
daily filters use) - never today's not-yet-closed session. No function
here ever looks at a bar timestamped after entry_ts.

Where the request names a metric with no standard, unambiguous industry
formula (Intraday Trend Score, Number of Pullbacks, Breakout Strength
Score, ES Opening Range Direction, Daily Trend Direction), this module
defines one explicitly and documents it - these are internally-consistent
heuristics for this system, not established indicators, and are flagged
as such in ENTRY_METRICS_KEYS' own comments and in the analysis report.

QQQ/SPY-dependent metrics (relative strength vs SPY/QQQ, SPY/QQQ VWAP
context) and Market Breadth are NOT implemented - this system has no
QQQ/SPY intraday data pipeline and no market-breadth data source at all
(see this conversation's own scoping decision). Those keys are still
present in every row (for a stable CSV/Excel schema) but always None.
"""
from __future__ import annotations

import pandas as pd

from src import es_filter, orb

# Same EWM-convergence reasoning as orb.py's own _CONFLUENCE_LOOKBACK_BARS
# (not imported - that constant is a private live-decision performance
# tuning knob; this module's own EMA/RSI values are diagnostic-only and
# deliberately kept independent of it), just generous enough that
# EMA20/RSI14 have both converged well past their own warm-up period.
_EWM_WARMUP_BARS = 400

_SESSION_OPEN = pd.Timestamp("09:30").time()

# Every key compute_entry_metrics ever returns, in report order - the
# single source of truth for perf.pair_trades (which fields to carry off
# the open leg), trades_xlsx.py/CSV export (column order), and
# analyze_entry_metrics.py (which columns are analyzable). "(heuristic)"
# marks a metric with no standard industry formula - see this module's own
# docstring. "(unavailable)" marks a QQQ/SPY/breadth metric always None.
ENTRY_METRICS_KEYS = [
    # Market Context
    "es_price", "es_vwap_dist_pct", "es_above_vwap",
    "es_or_direction",  # (heuristic)
    "es_trend_strength",  # (heuristic) slope of ES's own 9-EMA over the last 15 minutes
    "qqq_price", "qqq_vwap_dist_pct", "qqq_above_vwap",  # (unavailable)
    "spy_above_vwap",  # (unavailable)
    "market_breadth",  # (unavailable)
    # Gap Information
    "gap_pct", "gap_direction", "gap_size_category",
    # Relative Strength
    "stock_vs_spy_strength", "stock_vs_qqq_strength", "relative_strength_rank",  # (unavailable)
    "distance_from_daily_high_pct", "distance_from_daily_low_pct",
    # Volume Information
    "rvol", "volume_multiple_vs_avg_daily", "opening_range_volume",
    "breakout_candle_volume", "breakout_candle_volume_vs_avg",
    # Intraday Structure
    "entry_time_et", "minutes_from_open", "or_size_pct", "or_size_atr_units",
    "distance_from_vwap_pct", "distance_from_ema9_pct", "distance_from_ema20_pct",
    "distance_from_or_high_pct",
    # Trend Information
    "ema9_above_ema20", "price_above_ema9", "price_above_ema20",
    "intraday_trend_score",  # (heuristic) 0-5
    "consecutive_green_candles", "pullbacks_before_entry",  # (heuristic)
    # Volatility
    "atr_14", "atr_pct", "risk_width_pct", "risk_width_atr_ratio",
    # Breakout Quality
    "breakout_candle_range", "breakout_candle_range_atr_ratio",
    "breakout_candle_close_position", "breakout_strength_score",  # (heuristic)
    "breakout_retest_before_entry",
    # Daily Context
    "daily_trend_direction",  # (heuristic)
    "daily_close_above_ema20", "daily_close_above_ema50",
    "distance_from_52w_high_pct", "distance_from_prev_day_high_pct", "distance_from_prev_day_low_pct",
    # Session Performance
    "stock_perf_since_open_pct", "stock_perf_last_15m_pct", "stock_perf_last_30m_pct",
    "es_perf_since_open_pct", "qqq_perf_since_open_pct",  # qqq (unavailable)
]


def _pct(a: float, b: float) -> float | None:
    """(a - b) / b * 100 - None if b is zero/None (never divide by a
    degenerate reference level)."""
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100


def _price_n_minutes_ago(bars: pd.DataFrame, entry_ts, minutes: int) -> float | None:
    """Close of the last bar at or before entry_ts - minutes - None if no
    bar exists that far back yet (early in the session)."""
    if bars.empty:
        return None
    target = entry_ts - pd.Timedelta(minutes=minutes)
    prior = bars[bars.index <= target]
    return float(prior["Close"].iloc[-1]) if not prior.empty else None


def _es_context(es_bars_today: pd.DataFrame | None, entry_ts) -> dict:
    direction = es_filter.compute_market_direction(es_bars_today) if es_bars_today is not None else None
    if direction is None:
        return {
            "es_price": None, "es_vwap_dist_pct": None, "es_above_vwap": None,
            "es_or_direction": None, "es_trend_strength": None, "es_perf_since_open_pct": None,
        }
    es_price, es_vwap = direction["es_price"], direction["es_vwap"]

    # ES Opening Range Direction (heuristic): where ES's own 15-minute
    # opening range CLOSED relative to where it OPENED - a small dead-zone
    # (0.05% of price) keeps a near-flat OR from being labeled Bullish/
    # Bearish on noise alone.
    es_or_direction = None
    es_or = orb.compute_opening_range(es_bars_today)
    if es_or is not None:
        or_bars = es_bars_today[(es_bars_today.index.time >= orb.SESSION_OPEN_TIME) & (es_bars_today.index.time < orb.OR_END_TIME)]
        if not or_bars.empty:
            or_open, or_close = float(or_bars["Open"].iloc[0]), float(or_bars["Close"].iloc[-1])
            move_pct = _pct(or_close, or_open) or 0.0
            es_or_direction = "Bullish" if move_pct > 0.05 else "Bearish" if move_pct < -0.05 else "Neutral"

    # ES Trend Strength (heuristic): % change in ES's own 9-EMA over the
    # trailing 15 minutes (3 x 5-min bars), normalized to price so it's
    # comparable across ES's own price level over time.
    es_trend_strength = None
    ema9 = orb._compute_ema_series(es_bars_today["Close"], 9)
    ema9_15m_ago_bars = ema9[ema9.index <= entry_ts - pd.Timedelta(minutes=15)]
    if not ema9_15m_ago_bars.empty and not ema9.empty and pd.notna(ema9.iloc[-1]) and pd.notna(ema9_15m_ago_bars.iloc[-1]):
        es_trend_strength = _pct(float(ema9.iloc[-1]), float(ema9_15m_ago_bars.iloc[-1]))

    today_open = float(es_bars_today["Open"].iloc[0])
    return {
        "es_price": es_price, "es_vwap_dist_pct": _pct(es_price, es_vwap),
        "es_above_vwap": es_price > es_vwap,
        "es_or_direction": es_or_direction, "es_trend_strength": es_trend_strength,
        "es_perf_since_open_pct": _pct(es_price, today_open),
    }


def _gap_info(today_open: float, daily: pd.DataFrame) -> dict:
    if daily is None or daily.empty:
        return {"gap_pct": None, "gap_direction": None, "gap_size_category": None}
    prior_close = float(daily["Close"].iloc[-1])
    gap_pct = _pct(today_open, prior_close)
    if gap_pct is None:
        return {"gap_pct": None, "gap_direction": None, "gap_size_category": None}
    direction = "Gap Up" if gap_pct > 0 else "Gap Down" if gap_pct < 0 else "Flat"
    abs_gap = abs(gap_pct)
    category = "Small" if abs_gap < 1 else "Medium" if abs_gap <= 3 else "Large"
    return {"gap_pct": round(gap_pct, 3), "gap_direction": direction, "gap_size_category": category}


def _daily_context(daily: pd.DataFrame, entry_price: float) -> dict:
    """All off `daily`, which the caller guarantees ends at the prior
    COMPLETED trading day - never today's in-progress session."""
    empty = {
        "daily_trend_direction": None, "daily_close_above_ema20": None, "daily_close_above_ema50": None,
        "distance_from_52w_high_pct": None, "distance_from_prev_day_high_pct": None,
        "distance_from_prev_day_low_pct": None,
    }
    if daily is None or len(daily) < 2:
        return empty
    closes = daily["Close"]
    prior_close = float(closes.iloc[-1])
    prior_high, prior_low = float(daily["High"].iloc[-1]), float(daily["Low"].iloc[-1])

    ema20 = orb._compute_ema_series(closes, 20).iloc[-1] if len(closes) >= 20 else None
    ema50 = orb._compute_ema_series(closes, 50).iloc[-1] if len(closes) >= 50 else None
    ema20 = float(ema20) if ema20 is not None and pd.notna(ema20) else None
    ema50 = float(ema50) if ema50 is not None and pd.notna(ema50) else None

    trend = None
    if ema20 is not None and ema50 is not None:
        if prior_close > ema20 > ema50:
            trend = "Up"
        elif prior_close < ema20 < ema50:
            trend = "Down"
        else:
            trend = "Sideways"

    lookback_252 = closes.tail(252)
    high_252 = float(daily["High"].tail(252).max()) if len(lookback_252) else None

    return {
        "daily_trend_direction": trend,
        "daily_close_above_ema20": (prior_close > ema20) if ema20 is not None else None,
        "daily_close_above_ema50": (prior_close > ema50) if ema50 is not None else None,
        "distance_from_52w_high_pct": _pct(entry_price, high_252) if high_252 else None,
        "distance_from_prev_day_high_pct": _pct(entry_price, prior_high),
        "distance_from_prev_day_low_pct": _pct(entry_price, prior_low),
    }


def _consecutive_green_candles(today_bars: pd.DataFrame) -> int:
    """Trailing run of Close > Open candles ending at (and including) the
    last bar in `today_bars` - which is always the entry bar itself, since
    the caller already sliced to <= entry_ts."""
    count = 0
    for _, bar in today_bars.iloc[::-1].iterrows():
        if bar["Close"] > bar["Open"]:
            count += 1
        else:
            break
    return count


def _pullbacks_before_entry(today_bars: pd.DataFrame) -> int:
    """Heuristic: number of green-to-red direction changes among today's
    bars STRICTLY BEFORE the entry bar - each one is one completed
    up-leg-then-pause/reversal ("pullback") the stock made on its way to
    this entry. Not a swing-high/low pivot detector, just a same-candle-
    color reversal count - documented as a heuristic, not a standard
    pullback definition."""
    bars_before_entry = today_bars.iloc[:-1]
    if len(bars_before_entry) < 2:
        return 0
    is_green = (bars_before_entry["Close"] > bars_before_entry["Open"]).tolist()
    return sum(1 for i in range(1, len(is_green)) if is_green[i - 1] and not is_green[i])


def _volume_multiple_vs_avg_daily(today_bars: pd.DataFrame, prior_day_bars: dict | None) -> float | None:
    """Today's cumulative volume-so-far divided by the average FULL-DAY
    volume over up to the last 20 prior trading days available in
    `prior_day_bars` - deliberately a plain whole-day average (not
    orb._compute_rvol's own same-time-of-day-matched average), so this is
    a genuinely different number from RVOL, not a relabeled duplicate."""
    if not prior_day_bars:
        return None
    prior_dates = sorted(prior_day_bars.keys())[-20:]
    if not prior_dates:
        return None
    daily_totals = [float(prior_day_bars[d]["Volume"].sum()) for d in prior_dates]
    avg_daily_volume = sum(daily_totals) / len(daily_totals)
    if not avg_daily_volume:
        return None
    return float(today_bars["Volume"].sum()) / avg_daily_volume


def compute_entry_metrics(
    side: str, entry_price: float, entry_ts, initial_stop: float,
    today_bars: pd.DataFrame, intraday: pd.DataFrame, daily: pd.DataFrame,
    detail: dict, es_bars_today: pd.DataFrame | None,
    prior_day_bars: dict | None = None,
) -> dict:
    """`today_bars` must already be sliced to <= entry_ts (this symbol's
    own bars for entry_ts's calendar day only). `intraday` must already be
    sliced to <= entry_ts (this symbol's own continuous multi-day bars,
    for EMA/RSI convergence across session boundaries). `daily` must
    already end at entry_ts's PRIOR completed trading day. `prior_day_bars`
    is the same {date: that day's bars} dict backtest_engine.py already
    threads into orb.evaluate_orb_entry for RVOL (prior_day_bars_by_symbol
    [symbol]) - reused here for volume_multiple_vs_avg_daily's own
    lookback, deliberately NOT the strategy's own V1_rvol_lookback_days
    (this is a fixed, config-independent "typical day" reference, not
    another RVOL). `es_bars_today`
    must already be sliced to <= entry_ts (ES's own bars for the same
    calendar day), or None if no ES data was supplied to this backtest
    run at all. `detail` is orb.evaluate_orb_entry's own return dict for
    this exact entry (or_high/or_low/rvol/atr_pct/model already computed
    there, reused here rather than recomputed a second time)."""
    or_high, or_low = detail["or_high"], detail["or_low"]
    atr_pct = detail["atr_pct"]
    atr_dollars = atr_pct * entry_price / 100 if atr_pct else None
    today_open = float(today_bars["Open"].iloc[0])
    today_high = float(today_bars["High"].max())
    today_low = float(today_bars["Low"].min())

    # The confirm/breakout candle: for the "breakout" model it's the entry
    # bar itself (see orb.evaluate_orb_entry - breakout only ever fires on
    # current_ts == confirm_ts); for "retest" it's the earlier bar whose
    # Close first cleared the opening range, found the exact same way
    # evaluate_orb_entry itself does (post-OR bars, first Close beyond
    # or_high/or_low) - always strictly before the entry bar, so this is
    # already point-in-time safe by construction.
    signal_side = detail.get("signal_side") or side
    if detail.get("model") == "breakout":
        confirm_bar = today_bars.iloc[-1]
    else:
        or_range = orb.compute_opening_range(today_bars)
        post_or = today_bars[today_bars.index > or_range["or_end_ts"]] if or_range else today_bars.iloc[0:0]
        confirm_candidates = post_or[post_or["Close"] > or_high] if signal_side == "long" else post_or[post_or["Close"] < or_low]
        confirm_bar = confirm_candidates.iloc[0] if not confirm_candidates.empty else today_bars.iloc[-1]

    or_bars = today_bars[(today_bars.index.time >= orb.SESSION_OPEN_TIME) & (today_bars.index.time < orb.OR_END_TIME)]

    vwap_series = orb._compute_vwap_series(today_bars)
    vwap = float(vwap_series.iloc[-1]) if not vwap_series.empty and pd.notna(vwap_series.iloc[-1]) else None

    recent_closes = intraday["Close"].tail(_EWM_WARMUP_BARS)
    ema9_series = orb._compute_ema_series(recent_closes, 9)
    ema20_series = orb._compute_ema_series(recent_closes, 20)
    ema9 = float(ema9_series.iloc[-1]) if pd.notna(ema9_series.iloc[-1]) else None
    ema20 = float(ema20_series.iloc[-1]) if pd.notna(ema20_series.iloc[-1]) else None
    rsi_series = orb._compute_rsi_series(recent_closes, 14)
    rsi = float(rsi_series.iloc[-1]) if pd.notna(rsi_series.iloc[-1]) else None

    trend_checks = []
    if ema9 is not None:
        trend_checks.append(entry_price > ema9 if side == "long" else entry_price < ema9)
    if ema20 is not None:
        trend_checks.append(entry_price > ema20 if side == "long" else entry_price < ema20)
    if ema9 is not None and ema20 is not None:
        trend_checks.append(ema9 > ema20 if side == "long" else ema9 < ema20)
    if vwap is not None:
        trend_checks.append(entry_price > vwap if side == "long" else entry_price < vwap)
    if rsi is not None:
        trend_checks.append(rsi > 50 if side == "long" else rsi < 50)
    intraday_trend_score = sum(trend_checks) if trend_checks else None

    breakout_range = float(confirm_bar["High"] - confirm_bar["Low"])
    breakout_close_pos = ((confirm_bar["Close"] - confirm_bar["Low"]) / breakout_range) if breakout_range > 0 else None
    # Breakout Strength Score (heuristic, 0-100): average of (1) the
    # breakout candle's range relative to ATR (capped at 1.5x ATR = full
    # score, since a bigger-than-1.5x-ATR candle isn't "stronger" for this
    # purpose, just an outlier) and (2) where in its own range the candle
    # closed, side-adjusted (a strong long breakout closes near its HIGH;
    # a strong short breakdown closes near its LOW).
    breakout_strength_score = None
    if breakout_close_pos is not None and atr_dollars:
        range_component = min(breakout_range / (atr_dollars * 1.5), 1.0)
        close_component = breakout_close_pos if side == "long" else (1 - breakout_close_pos)
        breakout_strength_score = round((range_component + close_component) / 2 * 100, 1)

    risk_width_pct = abs(_pct(entry_price, initial_stop) or 0) if initial_stop else None
    risk_width_dollars = abs(entry_price - initial_stop)

    return {
        # Market Context (ES available when es_bars_today is supplied; QQQ/SPY/breadth always None - see module docstring)
        **_es_context(es_bars_today, entry_ts),
        "qqq_price": None, "qqq_vwap_dist_pct": None, "qqq_above_vwap": None,
        "spy_above_vwap": None, "market_breadth": None, "qqq_perf_since_open_pct": None,
        # Gap Information
        **_gap_info(today_open, daily),
        # Relative Strength
        "stock_vs_spy_strength": None, "stock_vs_qqq_strength": None, "relative_strength_rank": None,
        "distance_from_daily_high_pct": _pct(today_high, entry_price),
        "distance_from_daily_low_pct": _pct(entry_price, today_low),
        # Volume Information
        "rvol": detail.get("rvol"),
        "volume_multiple_vs_avg_daily": _volume_multiple_vs_avg_daily(today_bars, prior_day_bars),
        "opening_range_volume": float(or_bars["Volume"].sum()) if not or_bars.empty else None,
        "breakout_candle_volume": float(confirm_bar["Volume"]),
        "breakout_candle_volume_vs_avg": (
            float(confirm_bar["Volume"]) / today_bars["Volume"].mean()
            if today_bars["Volume"].mean() else None
        ),
        # Intraday Structure
        "entry_time_et": entry_ts.strftime("%H:%M"),
        "minutes_from_open": round((entry_ts - entry_ts.normalize().replace(hour=9, minute=30)).total_seconds() / 60, 1),
        "or_size_pct": _pct(or_high, or_low),
        "or_size_atr_units": ((or_high - or_low) / atr_dollars) if atr_dollars else None,
        "distance_from_vwap_pct": _pct(entry_price, vwap) if vwap else None,
        "distance_from_ema9_pct": _pct(entry_price, ema9) if ema9 else None,
        "distance_from_ema20_pct": _pct(entry_price, ema20) if ema20 else None,
        "distance_from_or_high_pct": _pct(entry_price, or_high),
        # Trend Information
        "ema9_above_ema20": (ema9 > ema20) if (ema9 is not None and ema20 is not None) else None,
        "price_above_ema9": (entry_price > ema9) if ema9 is not None else None,
        "price_above_ema20": (entry_price > ema20) if ema20 is not None else None,
        "intraday_trend_score": intraday_trend_score,
        "consecutive_green_candles": _consecutive_green_candles(today_bars),
        "pullbacks_before_entry": _pullbacks_before_entry(today_bars),
        # Volatility
        "atr_14": round(atr_dollars, 4) if atr_dollars else None,
        "atr_pct": atr_pct,
        "risk_width_pct": round(risk_width_pct, 3) if risk_width_pct is not None else None,
        "risk_width_atr_ratio": (risk_width_dollars / atr_dollars) if atr_dollars else None,
        # Breakout Quality
        "breakout_candle_range": round(breakout_range, 4),
        "breakout_candle_range_atr_ratio": (breakout_range / atr_dollars) if atr_dollars else None,
        "breakout_candle_close_position": round(breakout_close_pos, 3) if breakout_close_pos is not None else None,
        "breakout_strength_score": breakout_strength_score,
        "breakout_retest_before_entry": detail.get("model") == "retest",
        # Daily Context
        **_daily_context(daily, entry_price),
        # Session Performance
        "stock_perf_since_open_pct": _pct(entry_price, today_open),
        "stock_perf_last_15m_pct": _pct(entry_price, _price_n_minutes_ago(today_bars, entry_ts, 15)),
        "stock_perf_last_30m_pct": _pct(entry_price, _price_n_minutes_ago(today_bars, entry_ts, 30)),
    }
