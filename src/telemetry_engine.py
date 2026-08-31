"""Trade Telemetry Dashboard - a PASSIVE, read-only research framework that
collects, stores and analyzes technical-indicator behavior in the minutes
right after a backtest trade's own entry, to help spot early warning signs
that distinguish winners from losers, hard-stop trades, poor-Capture-%
trades, and trades that never reach trailing activation.

NEVER read by any strategy's entry/exit decision, position sizing, risk
model, hard-stop model, or trailing algorithm - this module only ever runs
AFTER a backtest has already produced its own trades (see generate_
telemetry_for_backtest, always called from a "Generate Telemetry" action
against an already-finished backtest row), purely to observe and report.
Nothing here can change what a strategy already did.

Architecture: generic over any backtest whose strategy is ORB-shaped (a
rules_json carrying "opening_range" - see backtest_runner.run_one_strategy's
own dispatch) or, for every non-structural indicator (RSI/MACD/Stochastic/
ADX/VWAP/EMA/volume/volatility/market-context/trade-progress), over ANY
strategy at all - a symbol, an entry timestamp/price/side and an initial
risk width (from perf.initial_risk_per_share) are all this module actually
needs. Only the "Trade Structure Events" section (opening-range/breakout/
retest references) is ORB-specific and is simply omitted (None) for a
non-ORB strategy. Attaching this to V4/V4.1/V4.3 or a future ORB variant
therefore needs no changes here at all - only a different strategy_id/
backtest_id passed into generate_telemetry_for_backtest.

Snapshot resolution: this module's only price/volume source is the SAME
5-minute intraday bar cache backtest_engine.py itself reads (src.
backtest_data) - there is no separate finer-grained data pipeline. Every
snapshot offset (+1m/+3m/+5m/+10m/+15m) resolves to "the last already-
closed 5-minute bar at or before entry_time + offset" (see _snapshot_bar) -
a +1m or +3m snapshot is very often IDENTICAL to the entry snapshot's own
bar (not yet a full 5-minute bar has closed since entry), which is the
honest answer at this data resolution, not a bug. Every stored snapshot
carries its own real bar timestamp and `minutes_elapsed` (which can differ
from the nominal offset for exactly this reason) so nothing here silently
overstates its own precision - see the "adapt to the existing 5-minute
resolution" scoping decision this feature was built against.
"""
from __future__ import annotations

import json
from datetime import date, datetime

import numpy as np
import pandas as pd

from src import backtest_data, backtest_engine, db, es_filter, orb, perf, technicals

SNAPSHOT_OFFSETS = [("entry", 0), ("1m", 1), ("3m", 3), ("5m", 5), ("10m", 10), ("15m", 15)]
_NON_ENTRY_LABELS = [label for label, _ in SNAPSHOT_OFFSETS if label != "entry"]

# Same EWM-convergence reasoning as src/entry_metrics.py's own _EWM_WARMUP_
# BARS (RSI(14)/EMA(9/20/50)/MACD/ADX all need enough trailing history to
# have actually converged) - an independent constant rather than importing
# entry_metrics' own private one, since this module's own indicator set
# (MACD/Stochastic/ADX/ATR/OBV) is entirely its own.
_WARMUP_BARS = 400

# Same "typical day" trailing-lookback convention entry_metrics.py's own
# _volume_multiple_vs_avg_daily already uses, reused here for RVOL (via
# orb._compute_rvol, the SAME formula the live strategy's own I3 filter and
# entry_metrics.py's own "rvol" already use - not a new invented one) and
# for this module's own "Volume vs Average" proxy.
RVOL_LOOKBACK_DAYS = 20

MARKET_CONTEXT_SYMBOLS = {"es": es_filter.ES_SYMBOL, "spy": "SPY", "qqq": "QQQ"}

# Default trade-grouping predicates for the Analysis Module / Comparison
# Engine (see the spec's own "Allow grouping trades into" section) - each
# takes one flattened trade record (see flatten_trades) and an optional
# cfg dict of thresholds. Deliberately kept as simple, named, independently
# togglable predicates over the SAME trade set rather than one rigid
# mutually-exclusive classification, since the spec's own "Winners"/
# "Losers" examples already aren't exhaustive (e.g. a trade closing exactly
# at breakeven falls in neither) - a dashboard can combine any of these
# (e.g. "Hard Stop" AND "Losers") rather than being forced to pick one.
def _early_failure_candidate(t, cfg) -> bool:
    """Research-only DERIVED group - "Trades that would have triggered the
    proposed 10-minute Early Failure rule." This is purely a retrospective
    LABEL computed from already-recorded +10m telemetry (see SNAPSHOT_
    OFFSETS/_indicator_snapshot/_structure_snapshot/_derive_since_entry_
    fields) - it does NOT touch, gate, or feed back into any strategy's
    own entry/exit logic, backtest run, or stored results in any way; a
    trade already closed exactly the way its own backtest run decided,
    long before this label is ever computed.

    Membership requires ALL of, evaluated strictly at the +10m snapshot:
      - trail_activated is False (the trade's own final outcome - same
        field never_trailed already keys off, not a per-snapshot value,
        since there's no "trailing activated as of this snapshot" fact to
        read - only "did it ever activate, by the time the trade closed")
      - 10m current_r <= -0.40R
      - 10m RSI Delta <= -5
      - EITHER 10m Returned Inside Opening Range OR 10m Lost EMA9

    `t.get(...)` (not `t[...]`) throughout - a trade whose own +10m
    snapshot came back None (see _snapshot_bar's own "no bar yet" case)
    simply has no 10m_* columns to read at all for THAT row; missing
    numeric values as NaN safely fail every "<=" comparison below (never
    raise), and the two boolean checks use "== 1" rather than bool(...)
    for the same reason - flatten_trades stores a bool snapshot field as
    int 0/1, and bool(float('nan')) is True in Python, which would
    otherwise silently treat "unknown" as "condition met"."""
    if t.get("trail_activated") is not False:
        return False
    current_r = t.get("10m_current_r")
    rsi_delta = t.get("10m_rsi_delta")
    if current_r is None or not (current_r <= -0.40):
        return False
    if rsi_delta is None or not (rsi_delta <= -5):
        return False
    return t.get("10m_returned_inside_opening_range") == 1 or t.get("10m_lost_ema9") == 1


DEFAULT_GROUPS = {
    "winners": lambda t, cfg: t["final_r"] is not None and t["final_r"] > 0,
    "losers": lambda t, cfg: t["final_r"] is not None and t["final_r"] < 0,
    "hard_stop": lambda t, cfg: t["exit_reason"] == "hard_stop",
    "trailing_winners": lambda t, cfg: bool(t["trail_activated"]) and t["final_r"] is not None and t["final_r"] > 0,
    "never_trailed": lambda t, cfg: not bool(t["trail_activated"]),
    "capture_disaster": lambda t, cfg: (
        t["capture_pct"] is not None and t["capture_pct"] < cfg.get("capture_disaster_threshold", -1000)
    ),
    "early_failure_candidate": _early_failure_candidate,
}

# Trade-level columns flatten_trades always carries alongside the per-
# snapshot metric columns - never treated as a predictive FEATURE by
# predictive_ranking/suggested_candidate_filters (they're the labels being
# predicted, or identifying metadata, not indicator inputs).
NON_FEATURE_COLUMNS = {
    "telemetry_id", "backtest_id", "strategy_id", "strategy_name", "symbol", "side",
    "entry_time", "exit_time", "final_r", "exit_reason", "trail_activated", "capture_pct",
}

# "1m"/"3m" are excluded from every STATISTICAL computation (Comparison,
# Predictive Ranking/Feature Importance/Mutual Information, Suggested
# Filters, and the heatmap metric picker - see feature_columns below) -
# NOT from storage or the per-trade drill-down view, which still show all
# 6 snapshots. Reason: at this module's own 5-minute bar resolution (see
# the module docstring's own "Snapshot resolution" section), a +1m or +3m
# target very often resolves to the SAME bar as the entry snapshot itself
# - sometimes it's a real, later bar (an entry near the end of its own
# 5-minute window), sometimes it's an exact duplicate of the entry row,
# and there is no way to tell which case a given row is in without
# checking its own minutes_elapsed by hand. Feeding that inconsistent mix
# into a Random Forest/Mutual Information comparison risks (a) implying a
# real measurement was taken 1/3 minutes after entry when it frequently
# wasn't, and (b) padding the feature set with near-duplicates of the
# entry columns, diluting the entry columns' own measured importance
# rather than adding real signal. +5m/+10m/+15m don't have this problem -
# a 5-minute bar has always fully closed by then.
EXCLUDED_SNAPSHOT_PREFIXES = ("1m_", "3m_")

# Same wording shown on the /telemetry page (next to the analysis tabs)
# and written into the Analysis Excel export's own Summary sheet - one
# string, so the explanation can never drift between the two surfaces.
EXCLUDED_SNAPSHOT_NOTE = (
    "1m and 3m telemetry snapshots are unavailable because the backtest data resolution "
    "does not support these offsets. They are excluded from all statistical analysis."
)


def _round(value, digits=4):
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(f) or np.isinf(f):
        return None
    return round(f, digits)


def _bool_or_none(value) -> bool | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return bool(value)


def _normalize_bars(bars: pd.DataFrame) -> pd.DataFrame:
    """Defensive read-time normalization for a cached bars file this
    module doesn't itself own - dedupes by timestamp (keep last, same
    convention src.backtest_data.save_cached_bars already applies on
    WRITE, but an older cache file can predate that logic, or a bug
    elsewhere could still write one) and coerces OHLCV to numeric
    (coercing an unparseable value to NaN rather than leaving a column
    object-dtype). Both a duplicate-timestamp index and an object-dtype
    OHLCV column make pandas' own EWM/rolling series calls (RSI/EMA/MACD/
    ADX/ATR/OBV) raise a hard-to-diagnose "DataError: No numeric types to
    aggregate" - confirmed by direct testing, not a guess. Every other
    reader of this same cache (backtest_engine.py) only ever replays
    narrow, single-day-sliced windows and is far less likely to ever
    surface either issue than this module, which is the first to compute
    indicator series across a symbol's ENTIRE cached history in one call."""
    bars = bars[~bars.index.duplicated(keep="last")].sort_index()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        bars[col] = pd.to_numeric(bars[col], errors="coerce")
    return bars


def _session_vwap(bars: pd.DataFrame) -> pd.Series:
    """Session VWAP over `bars`' full multi-day span, reset every trading
    day - reuses orb._compute_vwap_series (session-scoped) per calendar
    day and concatenates, the same convention backtest_engine.py's own
    per-day VWAP use already establishes."""
    return pd.concat([orb._compute_vwap_series(day_bars) for _, day_bars in bars.groupby(bars.index.date)])


def _coerce_numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Final defensive pass over an assembled indicator frame - coerces
    every column to numeric (pd.to_numeric, errors="coerce"), converting
    any lingering pd.NA/object-dtype column back to clean float64 NaN.
    Needed because orb._compute_vwap_series (reused here, not owned by
    this module) divides by cum_vol.replace(0, pd.NA) - which itself
    degrades that column to object dtype in current pandas (same
    DataError: No numeric types to aggregate technicals.py's own
    replace(0, np.nan) fix was for) - a plain float64 column is a no-op
    here, so this is always safe to apply."""
    for col in frame.columns:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _indicator_frame(bars: pd.DataFrame) -> pd.DataFrame:
    """Every per-bar indicator this module tracks for a TRADED symbol,
    computed ONCE over its full continuous multi-day bar history (not per
    trade/snapshot) - same "compute the series, not per query" discipline
    orb.py's own RVOL cache already established. Aligned to bars.index."""
    closes, highs, lows, volumes = bars["Close"], bars["High"], bars["Low"], bars["Volume"]
    frame = pd.DataFrame(index=bars.index)
    frame["close"], frame["high"], frame["low"], frame["volume"] = closes, highs, lows, volumes
    frame["rsi"] = orb._compute_rsi_series(closes, 14)
    frame["ema9"] = orb._compute_ema_series(closes, 9)
    frame["ema20"] = orb._compute_ema_series(closes, 20)
    frame["ema50"] = orb._compute_ema_series(closes, 50)
    macd = technicals.macd_series(closes)
    frame["macd"], frame["macd_signal"], frame["macd_hist"] = macd["macd"], macd["signal"], macd["histogram"]
    stoch = technicals.stochastic_series(highs, lows, closes)
    frame["stoch_k"], frame["stoch_d"] = stoch["percent_k"], stoch["percent_d"]
    adx = technicals.adx_series(highs, lows, closes)
    frame["adx"], frame["plus_di"], frame["minus_di"] = adx["adx"], adx["plus_di"], adx["minus_di"]
    frame["atr"] = technicals.atr_series(highs, lows, closes)
    frame["obv"] = technicals.obv_series(closes, volumes)
    frame["vwap"] = _session_vwap(bars)
    return _coerce_numeric_frame(frame)


def _market_context_frame(bars: pd.DataFrame, include_adx: bool) -> pd.DataFrame:
    """The lighter indicator set the spec wants for ES/SPY/QQQ market
    context (Price, RSI, [ADX for ES only], Above VWAP, Distance From
    VWAP) - a subset of _indicator_frame, kept separate so a market-context
    symbol's cache doesn't pay for MACD/Stochastic/OBV it's never asked for."""
    closes = bars["Close"]
    frame = pd.DataFrame(index=bars.index)
    frame["price"] = closes
    frame["rsi"] = orb._compute_rsi_series(closes, 14)
    frame["vwap"] = _session_vwap(bars)
    if include_adx:
        frame["adx"] = technicals.adx_series(bars["High"], bars["Low"], closes)["adx"]
    return _coerce_numeric_frame(frame)


def _snapshot_bar(frame: pd.DataFrame, entry_ts, offset_minutes: int):
    """The last row of `frame` at or before entry_ts + offset_minutes - see
    this module's own docstring on why this, not an exact-offset lookup, is
    the honest resolution for 5-minute data. None if no bar exists yet at
    or before that target (e.g. a very late-session entry's own +15m
    target running past the last cached bar)."""
    target = entry_ts + pd.Timedelta(minutes=offset_minutes)
    eligible = frame[frame.index <= target]
    return eligible.iloc[-1] if not eligible.empty else None


def _market_context_snapshot(mkt_frame: pd.DataFrame | None, entry_ts, offset_minutes: int) -> dict:
    empty = {"price": None, "rsi": None, "adx": None, "above_vwap": None, "dist_vwap_pct": None}
    if mkt_frame is None or mkt_frame.empty:
        return empty
    row = _snapshot_bar(mkt_frame, entry_ts, offset_minutes)
    if row is None:
        return empty
    price, vwap = float(row["price"]), row.get("vwap")
    has_vwap = vwap is not None and not pd.isna(vwap)
    return {
        "price": _round(price, 4),
        "rsi": _round(row.get("rsi"), 2),
        "adx": _round(row.get("adx"), 2) if "adx" in mkt_frame.columns else None,
        "above_vwap": _bool_or_none(price > vwap) if has_vwap else None,
        "dist_vwap_pct": _round((price - vwap) / vwap * 100, 3) if has_vwap and vwap else None,
    }


def _confirm_bar_low(today_bars_upto_entry: pd.DataFrame, or_high: float, model: str | None):
    """The breakout/confirm candle's own Low - for "breakout" model this is
    the entry bar itself (breakout only ever fires on current_ts ==
    confirm_ts, see orb.evaluate_orb_entry); for "retest" it's the earlier
    bar whose Close first cleared or_high, found the same way entry_
    metrics.py's own confirm_bar detection already does (not imported from
    there - that function computes several OTHER things this module
    doesn't need, and duplicating just the bar-lookup here keeps this
    module independent of entry_metrics' own ORB-Long-only assumptions).
    Long-only (or_high, not or_low) since every strategy this feature is
    scoped to (V4/V4.1/V4.2/V4.3) is ORB Long. None if today_bars_upto_
    entry is empty."""
    if today_bars_upto_entry.empty:
        return None, None
    if model == "breakout":
        confirm = today_bars_upto_entry.iloc[-1]
        return confirm.name, float(confirm["Low"])
    or_range = orb.compute_opening_range(today_bars_upto_entry)
    post_or = today_bars_upto_entry[today_bars_upto_entry.index > or_range["or_end_ts"]] if or_range else today_bars_upto_entry.iloc[0:0]
    candidates = post_or[post_or["Close"] > or_high]
    confirm = candidates.iloc[0] if not candidates.empty else today_bars_upto_entry.iloc[-1]
    return confirm.name, float(confirm["Low"])


def _structure_context(today_bars: pd.DataFrame, entry_ts, model: str | None) -> dict:
    """Everything _structure_snapshot needs that's constant across a
    trade's own snapshots (opening range, breakout candle low, retest low)
    - computed ONCE per trade, not per snapshot. None fields throughout if
    the opening range hasn't even formed by entry_ts (shouldn't happen for
    a real ORB entry, but a defensively-handled edge case, not an error)."""
    today_upto_entry = today_bars[today_bars.index <= entry_ts]
    or_range = orb.compute_opening_range(today_upto_entry)
    if or_range is None:
        return {"or_high": None, "or_low": None, "confirm_low": None, "retest_low": None}
    confirm_ts, confirm_low = _confirm_bar_low(today_upto_entry, or_range["or_high"], model)
    retest_low = None
    if model == "retest" and confirm_ts is not None:
        retest_bars = today_upto_entry[(today_upto_entry.index > confirm_ts)]
        retest_low = float(retest_bars["Low"].min()) if not retest_bars.empty else confirm_low
    return {"or_high": or_range["or_high"], "or_low": or_range["or_low"], "confirm_low": confirm_low, "retest_low": retest_low}


def _structure_snapshot(struct_ctx: dict, path: pd.DataFrame, bar_row) -> dict:
    """Trade Structure Events at one snapshot - `path` is the traded
    symbol's own OHLC frame from entry through this snapshot's bar
    (inclusive), used for the "since entry" running-high tracking."""
    if struct_ctx["or_high"] is None:
        return {
            "returned_inside_opening_range": None, "broke_breakout_candle_low": None,
            "broke_retest_low": None, "new_intraday_high_since_entry": None, "minutes_since_last_new_high": None,
        }
    price = float(bar_row["close"])
    running_max_high = path["high"].cummax()
    is_new_high = bool(path["high"].iloc[-1] >= running_max_high.iloc[-1])
    new_high_bars = path.index[path["high"] >= running_max_high]
    last_new_high_ts = new_high_bars[-1] if len(new_high_bars) else path.index[0]
    return {
        "returned_inside_opening_range": _bool_or_none(struct_ctx["or_low"] <= price <= struct_ctx["or_high"]),
        "broke_breakout_candle_low": (
            _bool_or_none(float(path["low"].min()) < struct_ctx["confirm_low"]) if struct_ctx["confirm_low"] is not None else None
        ),
        "broke_retest_low": (
            _bool_or_none(float(path["low"].min()) < struct_ctx["retest_low"]) if struct_ctx["retest_low"] is not None else None
        ),
        "new_intraday_high_since_entry": is_new_high,
        "minutes_since_last_new_high": round((bar_row.name - last_new_high_ts).total_seconds() / 60, 1),
    }


def _indicator_snapshot(frame: pd.DataFrame, entry_ts, entry_price: float, side: str, risk: float | None, bar_row) -> dict:
    """The Momentum/Trend/VWAP/EMA/Volatility/Trade-Progress fields at one
    snapshot bar - everything that does NOT depend on comparing this
    snapshot to the entry snapshot (see _derive_since_entry_fields for
    those - RSI Delta, VWAP/EMA crossing events, ADX Rising/Falling, ATR
    Expansion) or on ORB structure (see _structure_snapshot)."""
    price = float(bar_row["close"])
    sign = 1 if side == "long" else -1
    path = frame.loc[entry_ts:bar_row.name]
    high_since_entry, low_since_entry = float(path["high"].max()), float(path["low"].min())
    mfe_so_far = (high_since_entry - entry_price) if side == "long" else (entry_price - low_since_entry)
    mae_so_far = (entry_price - low_since_entry) if side == "long" else (high_since_entry - entry_price)
    total_volume_since_entry = float(path["volume"].sum())
    bullish_volume = float(path.loc[path["close"] >= path["close"].shift(1).fillna(path["close"]), "volume"].sum())
    bearish_volume = total_volume_since_entry - bullish_volume

    vwap, ema9, ema20, ema50 = bar_row.get("vwap"), bar_row.get("ema9"), bar_row.get("ema20"), bar_row.get("ema50")
    has_vwap = vwap is not None and not pd.isna(vwap)

    return {
        "bar_time": bar_row.name.isoformat(),
        "price": _round(price, 4),
        "rsi": _round(bar_row.get("rsi"), 2),
        "macd": _round(bar_row.get("macd"), 4), "macd_signal": _round(bar_row.get("macd_signal"), 4),
        "macd_hist": _round(bar_row.get("macd_hist"), 4),
        "stoch_k": _round(bar_row.get("stoch_k"), 2), "stoch_d": _round(bar_row.get("stoch_d"), 2),
        "adx": _round(bar_row.get("adx"), 2), "plus_di": _round(bar_row.get("plus_di"), 2), "minus_di": _round(bar_row.get("minus_di"), 2),
        "above_vwap": _bool_or_none(price > vwap) if has_vwap else None,
        "dist_vwap_pct": _round((price - vwap) / vwap * 100, 3) if has_vwap and vwap else None,
        "dist_vwap_r": _round((price - vwap) / risk, 3) if has_vwap and risk else None,
        "ema9": _round(ema9, 4),
        "dist_ema9_pct": _round((price - ema9) / ema9 * 100, 3) if ema9 and not pd.isna(ema9) else None,
        "above_ema9": _bool_or_none(price > ema9) if ema9 is not None and not pd.isna(ema9) else None,
        "ema20": _round(ema20, 4),
        "dist_ema20_pct": _round((price - ema20) / ema20 * 100, 3) if ema20 and not pd.isna(ema20) else None,
        "above_ema20": _bool_or_none(price > ema20) if ema20 is not None and not pd.isna(ema20) else None,
        "ema50": _round(ema50, 4),
        "dist_ema50_pct": _round((price - ema50) / ema50 * 100, 3) if ema50 and not pd.isna(ema50) else None,
        "above_ema50": _bool_or_none(price > ema50) if ema50 is not None and not pd.isna(ema50) else None,
        "obv": _round(bar_row.get("obv"), 1),
        "atr": _round(bar_row.get("atr"), 4),
        "bullish_volume": _round(bullish_volume, 1),
        "bearish_volume": _round(bearish_volume, 1),
        "total_volume_since_entry": _round(total_volume_since_entry, 1),
        "current_r": _round((price - entry_price) / risk * sign, 3) if risk else None,
        "mfe_r_so_far": _round(mfe_so_far / risk, 3) if risk else None,
        "mae_r_so_far": _round(mae_so_far / risk, 3) if risk else None,
        "current_profit_pct": _round((price - entry_price) / entry_price * 100 * sign, 3) if entry_price else None,
        "current_drawdown_pct": _round((high_since_entry - price) / high_since_entry * 100, 3) if side == "long" and high_since_entry else (
            _round((price - low_since_entry) / price * 100, 3) if side == "short" and price else None
        ),
    }


def _minutes_on_side(path: pd.DataFrame, above: bool) -> float:
    """Cumulative minutes (bar-to-bar) `path`'s own close spent above (or
    below) its own vwap column - walks consecutive bar gaps rather than
    just counting bars * 5, so this stays correct even where cached bars
    have a gap (a thin-volume symbol, a half day)."""
    mask = (path["close"] > path["vwap"]) if above else (path["close"] < path["vwap"])
    mask = mask.fillna(False)
    if len(path) < 2:
        return 0.0
    gaps = path.index.to_series().diff().dt.total_seconds().fillna(0) / 60
    return round(float(gaps[mask.values].sum()), 1)


def _derive_since_entry_fields(entry_snapshot: dict, snapshot: dict, frame: pd.DataFrame, entry_ts, bar_ts) -> dict:
    """Everything defined relative to the ENTRY snapshot's own values -
    RSI Delta, VWAP/EMA crossing events + minutes-on-each-side, ADX Rising/
    Falling, ATR Expansion. Applied to every non-entry snapshot; the entry
    snapshot itself gets a documented "baseline, not yet meaningful" all-
    None version (see build_trade_telemetry)."""
    path = frame.loc[entry_ts:bar_ts]
    entry_above_vwap, now_above_vwap = entry_snapshot.get("above_vwap"), snapshot.get("above_vwap")
    entry_above_ema9, now_above_ema9 = entry_snapshot.get("above_ema9"), snapshot.get("above_ema9")
    entry_above_ema20, now_above_ema20 = entry_snapshot.get("above_ema20"), snapshot.get("above_ema20")
    entry_ema9_gt_ema20 = (entry_snapshot["ema9"] > entry_snapshot["ema20"]) if entry_snapshot.get("ema9") is not None and entry_snapshot.get("ema20") is not None else None
    now_ema9_gt_ema20 = (snapshot["ema9"] > snapshot["ema20"]) if snapshot.get("ema9") is not None and snapshot.get("ema20") is not None else None
    entry_adx, now_adx = entry_snapshot.get("adx"), snapshot.get("adx")
    entry_atr, now_atr = entry_snapshot.get("atr"), snapshot.get("atr")

    return {
        "rsi_delta": _round(snapshot["rsi"] - entry_snapshot["rsi"], 2) if snapshot.get("rsi") is not None and entry_snapshot.get("rsi") is not None else None,
        "crossed_above_vwap": _bool_or_none(entry_above_vwap is False and now_above_vwap is True) if entry_above_vwap is not None and now_above_vwap is not None else None,
        "crossed_below_vwap": _bool_or_none(entry_above_vwap is True and now_above_vwap is False) if entry_above_vwap is not None and now_above_vwap is not None else None,
        "minutes_above_vwap": _minutes_on_side(path, above=True) if "vwap" in path.columns else None,
        "minutes_below_vwap": _minutes_on_side(path, above=False) if "vwap" in path.columns else None,
        "lost_ema9": _bool_or_none(entry_above_ema9 is True and now_above_ema9 is False) if entry_above_ema9 is not None and now_above_ema9 is not None else None,
        "lost_ema20": _bool_or_none(entry_above_ema20 is True and now_above_ema20 is False) if entry_above_ema20 is not None and now_above_ema20 is not None else None,
        "ema9_crossed_ema20": _bool_or_none(entry_ema9_gt_ema20 != now_ema9_gt_ema20) if entry_ema9_gt_ema20 is not None and now_ema9_gt_ema20 is not None else None,
        "adx_rising": _bool_or_none(now_adx > entry_adx) if now_adx is not None and entry_adx is not None else None,
        "adx_falling": _bool_or_none(now_adx < entry_adx) if now_adx is not None and entry_adx is not None else None,
        "atr_expansion": _round(now_atr / entry_atr, 3) if now_atr is not None and entry_atr not in (None, 0) else None,
    }


def build_trade_telemetry(
    frame: pd.DataFrame, today_bars: pd.DataFrame, market_frames: dict, prior_day_bars: dict,
    entry_ts, entry_price: float, side: str, risk: float | None, model: str | None,
) -> dict:
    """Builds the full {"entry": {...}, "1m": {...}, ...} snapshots dict
    for ONE closed trade - the core of the snapshot engine. `frame` is the
    traded symbol's own _indicator_frame (already computed once for the
    whole run, see generate_telemetry_for_backtest); `today_bars` is that
    same symbol's raw OHLCV bars for entry_ts's own calendar day (for RVOL
    and structure context); `market_frames` is {"es"|"spy"|"qqq":
    _market_context_frame|None}; `prior_day_bars` is {date: that day's raw
    bars}, the same shape backtest_engine.py already threads into orb.
    evaluate_orb_entry, reused here for orb._compute_rvol's own lookback."""
    struct_ctx = _structure_context(today_bars, entry_ts, model)
    intraday_upto = frame  # RSI/EMA/etc. converge over the full multi-day series already
    snapshots = {}
    snapshot_bar_ts = {}  # label -> original tz-aware bar_row.name, reused by the derive pass below
    # (never re-parsed from the stored "bar_time" ISO string - round-
    # tripping a zoneinfo-aware Timestamp through isoformat()/pd.Timestamp()
    # yields an equal-but-not-identical fixed-offset tzinfo, which pandas'
    # own .loc slicing then rejects against frame.index's zoneinfo tzinfo).
    for label, offset_minutes in SNAPSHOT_OFFSETS:
        bar_row = _snapshot_bar(frame, entry_ts, offset_minutes)
        if bar_row is None:
            snapshots[label] = None
            continue
        bar_ts = bar_row.name
        snapshot_bar_ts[label] = bar_ts
        snap = _indicator_snapshot(frame, entry_ts, entry_price, side, risk, bar_row)
        snap["minutes_elapsed"] = round((bar_ts - entry_ts).total_seconds() / 60, 1)
        today_upto_bar = today_bars[today_bars.index <= bar_ts]
        as_of_time = bar_ts.time()
        snap["rvol"] = _round(
            orb._compute_rvol(today_upto_bar, intraday_upto, bar_ts.date(), as_of_time, RVOL_LOOKBACK_DAYS, prior_day_bars)
            if not today_upto_bar.empty else None,
            3,
        )
        # "Volume vs Average": cumulative volume since entry against this
        # symbol's own long-run average per-bar volume over the same
        # number of elapsed bars - deliberately a DIFFERENT, simpler
        # reference than RVOL just above (which is time-of-day-matched
        # against prior days), so the two fields stay genuinely distinct
        # rather than one relabeling the other.
        bars_elapsed = max(1, len(frame.loc[entry_ts:bar_ts]))
        avg_bar_volume = float(frame["volume"].tail(_WARMUP_BARS).mean())
        snap["volume_vs_average"] = (
            _round(snap["total_volume_since_entry"] / (avg_bar_volume * bars_elapsed), 3) if avg_bar_volume else None
        )
        for key, mkt_frame in market_frames.items():
            snap[key] = _market_context_snapshot(mkt_frame, entry_ts, offset_minutes)
        snap.update(_structure_snapshot(struct_ctx, frame.loc[entry_ts:bar_ts], bar_row))
        snapshots[label] = snap

    entry_snapshot = snapshots.get("entry")
    if entry_snapshot is not None:
        entry_snapshot.update({
            "rsi_delta": 0.0, "crossed_above_vwap": False, "crossed_below_vwap": False,
            "minutes_above_vwap": 0.0, "minutes_below_vwap": 0.0,
            "lost_ema9": False, "lost_ema20": False, "ema9_crossed_ema20": False,
            "adx_rising": False, "adx_falling": False, "atr_expansion": 1.0,
        })
        for label in _NON_ENTRY_LABELS:
            if snapshots.get(label) is not None:
                snapshots[label].update(_derive_since_entry_fields(entry_snapshot, snapshots[label], frame, entry_ts, snapshot_bar_ts[label]))
    return snapshots


def generate_telemetry_for_backtest(account_id: int, backtest_id: int) -> dict:
    """The "Generate Telemetry" job (see run_telemetry.py) - builds and
    stores one trade_telemetry row per closed trade across every ORB
    strategy included in this backtest's own results. Re-running this for
    the same backtest_id first clears its prior telemetry rows (db.
    delete_trade_telemetry_for_backtest), so this is safe to re-run after a
    re-analysis without ever accumulating duplicates.

    Returns a summary dict: {"trades_processed", "trades_skipped",
    "skipped_reasons": {reason: count}}."""
    backtest = db.get_backtest(backtest_id)
    if backtest is None or backtest["account_id"] != account_id:
        raise ValueError(f"Backtest {backtest_id} not found for this account")
    if backtest["status"] != "done":
        raise ValueError(f"Backtest {backtest_id} is not done (status={backtest['status']})")

    db.delete_trade_telemetry_for_backtest(backtest_id)

    symbol_cache: dict[str, dict] = {}  # symbol -> {"frame", "today_bars_by_date", "prior_day_bars"}
    market_frame_cache: dict[str, pd.DataFrame | None] = {}
    for key, mkt_symbol in MARKET_CONTEXT_SYMBOLS.items():
        mkt_bars = backtest_data.load_cached_bars(mkt_symbol, backtest_engine.BAR_SIZE)
        try:
            market_frame_cache[key] = _market_context_frame(_normalize_bars(mkt_bars), include_adx=(key == "es")) if mkt_bars is not None else None
        except Exception:  # noqa: BLE001 - a bad ES/SPY/QQQ cache must not block generation for every trade
            market_frame_cache[key] = None

    trades_processed = 0
    skipped_reasons: dict[str, int] = {}
    sample_errors: list[str] = []  # first few "reason: ExceptionType: message" strings, for diagnosing WHY without reproducing
    rows_to_insert = []

    for strategy_id, result in (backtest["results"] or {}).items():
        if not isinstance(result, dict) or "pairs" not in result:
            continue
        strategy = db.get_strategy(int(strategy_id))
        strategy_name = result.get("strategy_name") or (strategy["name"] if strategy else f"Strategy {strategy_id}")

        for pair in result["pairs"]:
            symbol = pair["symbol"]
            side = pair["side"]
            entry_iso = pair["buy_time"] if side == "long" else pair["sell_time"]
            exit_iso = pair["sell_time"] if side == "long" else pair["buy_time"]
            entry_price = pair["open_price"]
            risk = perf.initial_risk_per_share(pair)

            if symbol not in symbol_cache:
                try:
                    bars = backtest_data.load_cached_bars(symbol, backtest_engine.BAR_SIZE)
                    if bars is not None and not bars.empty:
                        bars = _normalize_bars(bars)
                    symbol_cache[symbol] = None if bars is None or bars.empty else {
                        "frame": _indicator_frame(bars),
                        "prior_day_bars": dict(tuple(bars.groupby(bars.index.date))),
                    }
                except Exception as exc:  # noqa: BLE001 - one symbol's own bad/pathological cache must not fail the whole run
                    symbol_cache[symbol] = "error"
                    if len(sample_errors) < 5:
                        sample_errors.append(f"{symbol}: {type(exc).__name__}: {exc}")
            cached = symbol_cache[symbol]
            if cached is None:
                skipped_reasons["no_cached_bars"] = skipped_reasons.get("no_cached_bars", 0) + 1
                continue
            if cached == "error":
                skipped_reasons["symbol_processing_error"] = skipped_reasons.get("symbol_processing_error", 0) + 1
                continue

            entry_ts = pd.Timestamp(entry_iso)
            if entry_ts.tzinfo is None:
                entry_ts = entry_ts.tz_localize(cached["frame"].index.tz)
            else:
                entry_ts = entry_ts.tz_convert(cached["frame"].index.tz)
            if entry_ts not in cached["frame"].index and cached["frame"][cached["frame"].index <= entry_ts].empty:
                skipped_reasons["entry_before_cached_history"] = skipped_reasons.get("entry_before_cached_history", 0) + 1
                continue

            today_bars = cached["prior_day_bars"].get(entry_ts.date())
            if today_bars is None or today_bars.empty:
                skipped_reasons["no_bars_for_entry_day"] = skipped_reasons.get("no_bars_for_entry_day", 0) + 1
                continue

            # A single trade hitting a pandas edge case (e.g. a symbol's
            # cache carrying a pathological bar shape) must not crash an
            # entire multi-hundred-trade run - same per-item isolation
            # reasoning as fetch_backtest_data.run_fetch's own per-symbol
            # try/except and run_optimization's per-chunk isolation.
            try:
                snapshots = build_trade_telemetry(
                    cached["frame"], today_bars, market_frame_cache, cached["prior_day_bars"],
                    entry_ts, entry_price, side, risk, pair.get("model"),
                )
            except Exception as exc:  # noqa: BLE001 - see comment above
                skipped_reasons["trade_processing_error"] = skipped_reasons.get("trade_processing_error", 0) + 1
                if len(sample_errors) < 5:
                    sample_errors.append(f"{symbol} @ {entry_iso}: {type(exc).__name__}: {exc}")
                continue
            rows_to_insert.append({
                "account_id": account_id, "backtest_id": backtest_id,
                "strategy_id": int(strategy_id), "strategy_name": strategy_name,
                "symbol": symbol, "side": side, "entry_time": entry_iso, "exit_time": exit_iso,
                "final_r": pair.get("final_r"), "exit_reason": pair.get("exit_reason"),
                "trail_activated": bool(pair.get("trail_activated")), "capture_pct": pair.get("capture_pct"),
                "snapshots": snapshots,
            })
            trades_processed += 1

    if rows_to_insert:
        db.insert_trade_telemetry_rows(rows_to_insert)
    return {
        "trades_processed": trades_processed,
        "trades_skipped": sum(skipped_reasons.values()),
        "skipped_reasons": skipped_reasons,
        "sample_errors": sample_errors,
    }


# --------------------------------------------------------------- analysis ---
# Everything below reads back ALREADY-STORED trade_telemetry rows (see db.
# list_trade_telemetry) - no bars/IBKR access, pure pandas over what
# generate_telemetry_for_backtest already computed and saved. Kept in this
# same module (not a separate one) since "analyze the telemetry this system
# itself collected" is squarely still this feature's own scope, distinct
# from backtest analysis proper.

def flatten_trades(rows: list[dict]) -> pd.DataFrame:
    """One row per trade, one column per "<snapshot>_<metric>" (e.g.
    "5m_rsi", "entry_adx", "10m_es_above_vwap") plus the trade-level
    columns in NON_FEATURE_COLUMNS - the flat feature matrix every
    comparison/ranking/heatmap/export function below works from. A
    snapshot that came back None (see _snapshot_bar) simply contributes no
    columns for that trade - NaN there, not a fabricated 0."""
    records = []
    for t in rows:
        rec = {
            "telemetry_id": t["id"], "backtest_id": t["backtest_id"], "strategy_id": t["strategy_id"],
            "strategy_name": t["strategy_name"], "symbol": t["symbol"], "side": t["side"],
            "entry_time": t["entry_time"], "exit_time": t["exit_time"],
            "final_r": t["final_r"], "exit_reason": t["exit_reason"],
            "trail_activated": bool(t["trail_activated"]), "capture_pct": t["capture_pct"],
        }
        for snap_label, snap in (t.get("snapshots") or {}).items():
            if not snap:
                continue
            for k, v in snap.items():
                if k == "bar_time":
                    continue
                if isinstance(v, dict):  # es/spy/qqq market-context sub-dicts
                    for kk, vv in v.items():
                        rec[f"{snap_label}_{k}_{kk}"] = int(vv) if isinstance(vv, bool) else vv
                elif isinstance(v, bool):
                    rec[f"{snap_label}_{k}"] = int(v)
                else:
                    rec[f"{snap_label}_{k}"] = v
        records.append(rec)
    return pd.DataFrame.from_records(records)


def feature_columns(df: pd.DataFrame, explicit: list[str] | None = None) -> list[str]:
    """Every column comparison_table/predictive_ranking/suggested_
    candidate_filters/the heatmap metric picker treat as a usable
    indicator feature - always excludes EXCLUDED_SNAPSHOT_PREFIXES (1m/3m,
    see its own docstring) regardless of whether `explicit` was passed, so
    no caller (present or future) has to remember to leave them out
    itself."""
    candidates = [c for c in explicit if c in df.columns] if explicit else [
        c for c in df.columns if c not in NON_FEATURE_COLUMNS and pd.api.types.is_numeric_dtype(df[c])
    ]
    return [c for c in candidates if not c.startswith(EXCLUDED_SNAPSHOT_PREFIXES)]


def apply_group(df: pd.DataFrame, group_key: str, cfg: dict | None = None) -> pd.Series:
    """Boolean mask over `df`'s rows for one of DEFAULT_GROUPS - `cfg`
    only matters for "capture_disaster" (its own configurable threshold,
    see the spec's own "Capture Disaster Group... Configurable" note)."""
    predicate = DEFAULT_GROUPS[group_key]
    cfg = cfg or {}
    return df.apply(lambda row: bool(predicate(row, cfg)), axis=1)


def metric_stats(series: pd.Series) -> dict:
    """Mean/median/std/percentiles for one metric column - Section
    "Statistical Ranking". None throughout for an empty (all-NaN) series,
    not a fabricated 0."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"count": 0, "mean": None, "median": None, "std": None, "p10": None, "p25": None, "p75": None, "p90": None}
    return {
        "count": int(len(s)), "mean": _round(s.mean(), 4), "median": _round(s.median(), 4),
        "std": _round(s.std(), 4) if len(s) > 1 else 0.0,
        "p10": _round(s.quantile(0.10), 4), "p25": _round(s.quantile(0.25), 4),
        "p75": _round(s.quantile(0.75), 4), "p90": _round(s.quantile(0.90), 4),
    }


def comparison_table(df: pd.DataFrame, mask_a: pd.Series, mask_b: pd.Series, columns: list[str] | None = None) -> list[dict]:
    """Section "Comparison Engine" - one row per metric, each group's own
    mean/median/count side by side."""
    cols = feature_columns(df, columns)
    rows = []
    for col in cols:
        stats_a, stats_b = metric_stats(df.loc[mask_a, col]), metric_stats(df.loc[mask_b, col])
        rows.append({
            "metric": col,
            "group_a_mean": stats_a["mean"], "group_a_median": stats_a["median"], "group_a_count": stats_a["count"],
            "group_b_mean": stats_b["mean"], "group_b_median": stats_b["median"], "group_b_count": stats_b["count"],
        })
    return rows


def predictive_ranking(df: pd.DataFrame, mask_a: pd.Series, mask_b: pd.Series, columns: list[str] | None = None) -> list[dict]:
    """Section "Predictive Power Ranking": Mutual Information + Random
    Forest feature importance for separating group A from group B (e.g.
    Hard Stop vs Trailing Winner). Requires scikit-learn (see requirements.
    txt) - returns [] rather than raising if it isn't installed, since this
    is a research/analysis feature, never load-bearing for anything else in
    the dashboard. Missing values are median-imputed per column (sklearn
    can't take NaN directly) - a column that's ALL NaN for both groups is
    dropped rather than imputed with a meaningless constant."""
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.feature_selection import mutual_info_classif
    except ImportError:
        return []
    cols = feature_columns(df, columns)
    sub = df.loc[mask_a | mask_b, cols].copy()
    labels = mask_a.loc[sub.index].astype(int)
    sub = sub.dropna(axis=1, how="all")
    cols = list(sub.columns)
    if sub.empty or len(sub) < 6 or labels.nunique() < 2 or not cols:
        return []
    sub = sub.apply(lambda col: col.fillna(col.median()))
    mi = mutual_info_classif(sub.values, labels.values, random_state=0)
    rf = RandomForestClassifier(n_estimators=200, max_depth=4, random_state=0, min_samples_leaf=max(2, len(sub) // 20))
    rf.fit(sub.values, labels.values)
    ranking = [
        {"metric": col, "mutual_information": _round(m, 4), "feature_importance": _round(imp, 4)}
        for col, m, imp in zip(cols, mi, rf.feature_importances_)
    ]
    ranking.sort(key=lambda r: r["feature_importance"], reverse=True)
    return ranking


def heatmap_data(df: pd.DataFrame, metric_col: str, r_col: str = "final_r", metric_bins: int = 6) -> dict:
    """Section "Indicator Heatmaps": a metric-value-bucket x Final-R-bucket
    2D count grid, for a client-side heatmap render. Empty grid (not an
    error) if `metric_col` has too little spread/data to bucket."""
    sub = df[[metric_col, r_col]].dropna()
    if sub.empty or sub[metric_col].nunique() < 2:
        return {"metric": metric_col, "x_labels": [], "y_labels": [], "matrix": []}
    r_edges = [-float("inf"), -2, -1, 0, 1, 2, 3, float("inf")]
    r_labels = ["<=-2R", "-2R..-1R", "-1R..0R", "0R..1R", "1R..2R", "2R..3R", ">3R"]
    try:
        metric_bucket = pd.qcut(sub[metric_col], q=min(metric_bins, sub[metric_col].nunique()), duplicates="drop")
    except ValueError:
        return {"metric": metric_col, "x_labels": [], "y_labels": [], "matrix": []}
    r_bucket = pd.cut(sub[r_col], bins=r_edges, labels=r_labels)
    table = pd.crosstab(metric_bucket, r_bucket, dropna=False)
    return {
        "metric": metric_col,
        "x_labels": [str(i) for i in table.columns],
        "y_labels": [str(i) for i in table.index],
        "matrix": table.values.tolist(),
    }


def suggested_candidate_filters(df: pd.DataFrame, mask_a: pd.Series, mask_b: pd.Series, label_a: str, label_b: str, top_n: int = 5) -> list[dict]:
    """Section "Suggested Candidate Filters": for each of the top-N most
    predictive metrics (by feature importance), a simple median-threshold
    split and the % of each group on the "worse" side of it - the spec's
    own worked-example shape ("ADX < 18 after 10 minutes appears in 72% of
    Hard Stop trades and only 21% of Trailing Winners"). Purely
    descriptive/statistical - "Do NOT modify strategy" per the spec, this
    never writes back to any strategy's rules_json."""
    ranking = predictive_ranking(df, mask_a, mask_b)[:top_n]
    suggestions = []
    for r in ranking:
        col = r["metric"]
        combined = df.loc[mask_a | mask_b, col].dropna()
        if combined.empty:
            continue
        threshold = float(combined.median())
        a_vals, b_vals = df.loc[mask_a, col].dropna(), df.loc[mask_b, col].dropna()
        a_pct = _round(float((a_vals < threshold).mean() * 100), 1) if len(a_vals) else None
        b_pct = _round(float((b_vals < threshold).mean() * 100), 1) if len(b_vals) else None
        suggestions.append({
            "metric": col, "threshold": _round(threshold, 3), "direction": "below",
            f"{label_a}_pct": a_pct, f"{label_b}_pct": b_pct,
            "feature_importance": r["feature_importance"], "mutual_information": r["mutual_information"],
        })
    return suggestions


def early_failure_analysis(df: pd.DataFrame, mask_hard_stop: pd.Series, mask_trailing_winner: pd.Series) -> dict:
    """Section "Early Failure Analysis": at 5/10/15 minutes, the strongest
    indicators separating Hard Stop trades from Trailing Winners - one
    predictive_ranking call per offset, restricted to that offset's own
    columns only (so "strongest at +10m" can't be answered by a +15m
    column leaking in)."""
    result = {}
    for label in ("5m", "10m", "15m"):
        prefix = f"{label}_"
        cols = [c for c in df.columns if c.startswith(prefix) and c not in NON_FEATURE_COLUMNS]
        result[label] = predictive_ranking(df, mask_hard_stop, mask_trailing_winner, cols)[:10]
    return result


def export_dataframe(df: pd.DataFrame, fmt: str, path):
    """CSV/Excel export of the flattened trade matrix (see flatten_trades)
    - `fmt` is "csv" or "xlsx"."""
    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "xlsx":
        df.to_excel(path, index=False, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported export format: {fmt}")


def export_analysis_xlsx(payload: dict, scope_label: str, group_a_label: str, group_b_label: str) -> bytes:
    """The full Analysis / Comparison Engine result (see web/app.py's own
    _build_analysis_payload, which computes exactly this dict for both the
    GET /api/telemetry/analysis JSON endpoint and this export) as a
    multi-sheet .xlsx workbook - one sheet per section, real numeric cells
    (sortable/filterable in Excel), same styling convention as src.
    trades_xlsx.build_trades_xlsx. Unlike export_dataframe (the raw
    per-trade snapshot matrix), this is the COMPUTED analysis itself -
    comparison table, predictive ranking, early failure analysis, and
    suggested filters - exactly what the dashboard's own 4 analysis tabs
    show, for offline reference or sharing outside the dashboard."""
    from io import BytesIO

    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    header_fill = PatternFill(start_color="EEF1F5", end_color="EEF1F5", fill_type="solid")
    header_font = Font(bold=True)

    def autosize(ws, ncols):
        for col in range(1, ncols + 1):
            letter = get_column_letter(col)
            width = max((len(str(c.value)) for c in ws[letter] if c.value is not None), default=8)
            ws.column_dimensions[letter].width = min(max(width + 2, 8), 40)

    def write_table(ws, rows: list[dict], columns: list[str] | None = None):
        if not rows:
            ws.append(["No data"])
            return
        cols = columns or list(rows[0].keys())
        ws.append(cols)
        for cell in ws[ws.max_row]:
            cell.font, cell.fill = header_font, header_fill
        for row in rows:
            ws.append([row.get(c) for c in cols])
        ws.freeze_panes = "A2"
        autosize(ws, len(cols))

    wb = Workbook()

    summary_ws = wb.active
    summary_ws.title = "Summary"
    summary_ws.append(["Trade Telemetry - Analysis / Comparison Engine"])
    summary_ws["A1"].font = Font(bold=True, size=14)
    summary_ws.append([f"Scope: {scope_label} | Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
    summary_ws.append([])
    summary_ws.append([EXCLUDED_SNAPSHOT_NOTE])
    summary_ws[summary_ws.max_row][0].font = Font(italic=True)
    summary_ws[summary_ws.max_row][0].alignment = Alignment(wrap_text=True)
    summary_ws.append([])
    summary_ws.append(["Metric", "Value"])
    for cell in summary_ws[summary_ws.max_row]:
        cell.font, cell.fill = header_font, header_fill
    summary_ws.append(["Total trades in scope", payload["trade_count"]])
    summary_ws.append([f"Group A ({group_a_label})", payload["group_a_count"]])
    summary_ws.append([f"Group B ({group_b_label})", payload["group_b_count"]])
    summary_ws.append([])
    summary_ws.append(["All group counts"])
    summary_ws[summary_ws.max_row][0].font = Font(bold=True)
    for key, count in payload["group_counts"].items():
        summary_ws.append([key, count])
    autosize(summary_ws, 2)

    write_table(wb.create_sheet("Comparison"), payload["comparison"])
    write_table(wb.create_sheet("Predictive Ranking"), payload["predictive_ranking"])

    early_ws = wb.create_sheet("Early Failure Analysis")
    early_rows = [
        {"offset": offset, **row}
        for offset, rows in payload["early_failure_analysis"].items()
        for row in rows
    ]
    write_table(early_ws, early_rows, columns=["offset", "metric", "feature_importance", "mutual_information"] if early_rows else None)

    write_table(wb.create_sheet("Suggested Filters"), payload["suggested_filters"])

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
