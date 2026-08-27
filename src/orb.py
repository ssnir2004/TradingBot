"""ORB (Opening Range Breakout) strategy: pure decision logic, shared by
the live cycle (cycle.py) and the backtester (src/backtest_engine.py) -
same "one implementation, two callers" pattern cycle._evaluate_filters_
from_bars already uses for the D1-D3/I1-I3 strategies, so the live bot
and the backtester can never quietly drift apart. See docs/orb_strategy_
spec.md for the full rules definition this implements.

Deliberately self-contained (no import of cycle.py) to avoid a circular
import, since cycle.py imports this module - the small ATR/RVOL helpers
below are intentional near-duplicates of cycle.py's own
_compute_atr/I3 logic, not a shared abstraction, to keep this module
independently testable and to avoid touching the live D1-D3/I1-I3 path
at all while adding this.

Rules shape (see EXTRA_STRATEGY_PRESETS in src/db.py for the full ORB
Long/ORB Short presets):
    {
      "opening_range": {"or_timeframe": "15m", "confirm_timeframe": "5m",
                         "entry_timeframe": "5m", "session_open_et": "09:30"},
      "volatility_filters": {"V1_rvol_min": 2.0, "V1_rvol_lookback_days": 14,
                              "V2_atr_period": 14, "V2_atr_pct_tiers": [...]},
      "entry_models": {"breakout": {"enabled": true, "target_rr": 2.0},
                        "retest": {"enabled": true, "target_rr": 2.0}},
      "time_filter": {...}, "exit": {"management_style": "fixed_target_no_trail"},
      "risk": {...},
    }
A strategy is dispatched here (instead of cycle._evaluate_filters_from_bars)
whenever its rules dict has an "opening_range" key - see cycle.entry_scan.

KNOWN LIMITATION (documented, not fixed, in stage 2): the breakout model
only fires on the EXACT bar that confirms the opening-range break (it
needs that bar's own gap/displacement, per the video's "candle that
formed the gap"). Live evaluation runs on a wall-clock schedule (a tick
a few minutes after a bar has closed), so it naturally observes that bar
as the latest one once it's done. A backtest's tick loop instead visits
bars AT their own label time, so a confirmation on the very FIRST
possible post-opening-range bar (9:45) can be gated out by
time_filter.earliest_entry_et (09:50) in backtest even though live can
still catch it. Only affects that one earliest edge case, and only the
breakout model (retest has no such single-bar timing requirement).
"""
from datetime import time as dt_time

import pandas as pd

SESSION_OPEN_TIME = dt_time(9, 30)
OR_END_TIME = dt_time(9, 45)  # 3 x 5-minute bars (9:30, 9:35, 9:40) -> a 15-minute opening range


def compute_opening_range(today_bars: pd.DataFrame) -> dict | None:
    """today_bars: one symbol's 5-minute bars for a single trading day
    (may extend past 9:45, or not yet reach it). Returns {"or_high",
    "or_low", "or_end_ts"} once all 3 opening-range bars exist, else None
    - "not enough bars yet" (before 9:45 ET) is a normal, common state,
    not an error."""
    or_bars = today_bars[(today_bars.index.time >= SESSION_OPEN_TIME) & (today_bars.index.time < OR_END_TIME)]
    if len(or_bars) < 3:
        return None
    return {
        "or_high": float(or_bars["High"].max()),
        "or_low": float(or_bars["Low"].min()),
        "or_end_ts": or_bars.index[-1],
    }


def _atr_pct_tier_min(price: float, tiers: list[dict]) -> float | None:
    """The ATR% minimum for whichever price tier `price` falls into (see
    docs/orb_strategy_spec.md's tier table) - None if price falls outside
    every tier (shouldn't happen given the tiers span $3 to infinity, but
    a strategy predating/misconfiguring this field must not crash)."""
    for tier in tiers:
        lo, hi = tier["price_min"], tier["price_max"]
        if price >= lo and (hi is None or price < hi):
            return tier["atr_pct_min"]
    return None


def _compute_atr(daily: pd.DataFrame, period: int) -> float | None:
    """Wilder's ATR as of the last COMPLETE trading day in `daily` -
    excludes daily.iloc[-1] (today, still in progress). Intentional
    near-duplicate of cycle._compute_atr - see this module's own
    docstring for why it isn't imported instead."""
    completed = daily.iloc[:-1]
    if len(completed) < period + 1:
        return None
    high, low, close = completed["High"], completed["Low"], completed["Close"]
    prev_close = close.shift(1)
    true_range = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    value = true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean().iloc[-1]
    return float(value) if pd.notna(value) else None


def _compute_rvol(
    today_bars: pd.DataFrame, intraday: pd.DataFrame, as_of_date, as_of_time, lookback: int,
    prior_day_bars: dict | None = None,
) -> float:
    """Today's volume-so-far against the average volume accumulated by
    this same time-of-day over the past `lookback` trading days -
    intentional near-duplicate of cycle._evaluate_filters_from_bars' I3
    logic (same reasoning as _compute_atr above)."""
    if prior_day_bars is not None:
        prior_dates = sorted(prior_day_bars.keys())[-lookback:]
        prior_volume_by_this_time = [
            float(prior_day_bars[d][prior_day_bars[d].index.time <= as_of_time]["Volume"].sum())
            for d in prior_dates
        ]
    else:
        prior_dates = sorted({d for d in intraday.index.date if d < as_of_date})[-lookback:]
        prior_volume_by_this_time = [
            float(intraday[(intraday.index.date == d) & (intraday.index.time <= as_of_time)]["Volume"].sum())
            for d in prior_dates
        ]
    avg_volume = (sum(prior_volume_by_this_time) / len(prior_volume_by_this_time)) if prior_volume_by_this_time else 0.0
    today_volume_so_far = float(today_bars["Volume"].sum())
    return today_volume_so_far / avg_volume if avg_volume else 0.0


def evaluate_orb_entry(
    daily: pd.DataFrame, intraday: pd.DataFrame, rules: dict, side: str,
    prior_day_bars: dict | None = None,
) -> dict:
    """The ORB decision logic - no data fetching, no wall-clock "now": the
    day being evaluated is whatever the last date in `intraday`'s index
    is, and `daily` must already end at that day's prior trading day
    (exact same calling convention as cycle._evaluate_filters_from_bars,
    so cycle.py and backtest_engine.py can share this one implementation).

    Returns a dict always carrying "pass" (bool) and "side". On
    insufficient data it instead carries "error" (mirrors
    _evaluate_filters_from_bars' own early-return shape) - caller code
    should treat "error" entries as "not yet evaluable", not as a failed
    check. Once evaluable, also carries "or_formed"/"confirmed"/
    "volatility_ok" (independent diagnostic flags, for filter_stats) and,
    on pass: "model" ("breakout"|"retest"), "price" (entry price),
    "initial_stop", "target_price".

    Unlike _evaluate_filters_from_bars this doesn't call
    cycle._resolve_initial_stop - the entry model itself determines the
    stop (the gap candle's own low/high, or the retest bar's swing
    low/high), not one of INITIAL_STOP_RULES' generic session-extreme/
    ATR-multiple rules. Callers must read "initial_stop"/"target_price"
    directly off this function's own result instead."""
    vol_filters = rules["volatility_filters"]
    entry_models = rules["entry_models"]

    if intraday.empty:
        return {"pass": False, "side": side, "error": "no intraday data"}
    as_of_date = intraday.index[-1].date()
    today_bars = intraday[intraday.index.date == as_of_date]
    if today_bars.empty:
        return {"pass": False, "side": side, "error": "no bars for today yet"}

    current_ts = today_bars.index[-1]
    current_price = float(today_bars["Close"].iloc[-1])

    or_range = compute_opening_range(today_bars)
    if or_range is None:
        return {"pass": False, "side": side, "error": "opening range not yet formed"}
    or_high, or_low, or_end_ts = or_range["or_high"], or_range["or_low"], or_range["or_end_ts"]

    atr_value = _compute_atr(daily, vol_filters.get("V2_atr_period", 14))
    if atr_value is None:
        return {"pass": False, "side": side, "error": "not enough daily history for ATR"}
    atr_pct = (atr_value / current_price * 100) if current_price else 0.0
    atr_tier_min = _atr_pct_tier_min(current_price, vol_filters["V2_atr_pct_tiers"])
    atr_ok = atr_tier_min is not None and atr_pct >= atr_tier_min

    as_of_time = current_ts.time()
    rvol = _compute_rvol(today_bars, intraday, as_of_date, as_of_time, vol_filters["V1_rvol_lookback_days"], prior_day_bars)
    rvol_ok = rvol >= vol_filters["V1_rvol_min"]
    volatility_ok = bool(atr_ok and rvol_ok)

    post_or_bars = today_bars[today_bars.index > or_end_ts]
    if side == "long":
        confirm_bars = post_or_bars[post_or_bars["Close"] > or_high]
    else:
        confirm_bars = post_or_bars[post_or_bars["Close"] < or_low]
    confirmed = not confirm_bars.empty
    confirm_ts = confirm_bars.index[0] if confirmed else None

    detail = {
        "side": side, "price": current_price, "or_high": or_high, "or_low": or_low,
        "rvol": rvol, "atr_pct": atr_pct, "atr_tier_min": atr_tier_min,
        "or_formed": True, "confirmed": confirmed, "volatility_ok": volatility_ok,
    }

    if not volatility_ok or not confirmed:
        return {"pass": False, **detail}

    # --- breakout: only exactly at the confirmation bar itself, and only
    # if that bar shows a clean displacement gap off the prior bar (the
    # video's "candle that formed the gap") ---
    if entry_models.get("breakout", {}).get("enabled") and current_ts == confirm_ts:
        bar_pos = today_bars.index.get_loc(confirm_ts)
        if bar_pos > 0:
            prev_bar = today_bars.iloc[bar_pos - 1]
            confirm_bar = today_bars.loc[confirm_ts]
            gap = (confirm_bar["Low"] > prev_bar["High"]) if side == "long" else (confirm_bar["High"] < prev_bar["Low"])
            if gap:
                entry_price = float(confirm_bar["Close"])
                stop = float(confirm_bar["Low"]) if side == "long" else float(confirm_bar["High"])
                risk = (entry_price - stop) if side == "long" else (stop - entry_price)
                if risk > 0:
                    target_rr = entry_models["breakout"]["target_rr"]
                    target = entry_price + target_rr * risk if side == "long" else entry_price - target_rr * risk
                    return {"pass": True, "model": "breakout", "initial_stop": stop, "target_price": target,
                            **detail, "price": entry_price}

    # --- retest: any bar strictly after confirmation that dips back to the
    # opening-range level and closes back on the breakout side (holds it) ---
    if entry_models.get("retest", {}).get("enabled") and current_ts > confirm_ts:
        bar = today_bars.loc[current_ts]
        if side == "long":
            retest_hit = bar["Low"] <= or_high and bar["Close"] > or_high and bar["Close"] > bar["Open"]
        else:
            retest_hit = bar["High"] >= or_low and bar["Close"] < or_low and bar["Close"] < bar["Open"]
        if retest_hit:
            entry_price = float(bar["Close"])
            stop = float(bar["Low"]) if side == "long" else float(bar["High"])
            risk = (entry_price - stop) if side == "long" else (stop - entry_price)
            if risk > 0:
                target_rr = entry_models["retest"]["target_rr"]
                target = entry_price + target_rr * risk if side == "long" else entry_price - target_rr * risk
                return {"pass": True, "model": "retest", "initial_stop": stop, "target_price": target,
                        **detail, "price": entry_price}

    return {"pass": False, **detail}


def fixed_target_decision(pos: dict, price: float, side: str) -> dict:
    """Pure decision logic for a "fixed_target_no_trail" position (see
    cycle.manage_position's ORB branch) - the mirror of cycle._breakeven_
    decision/_trailing_stop_decision for strategies that exit fully at a
    fixed R:R target instead of trailing. The stop side is handled
    entirely by the broker-side stop order already placed at entry (same
    as every other strategy) - this only ever decides the TARGET side."""
    target = pos.get("target_price")
    if target is None:
        return {"action": "hold"}
    hit = (price <= target) if side == "short" else (price >= target)
    return {"action": "close_target"} if hit else {"action": "hold"}
