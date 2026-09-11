"""SST Swing - a daily-chart, multi-day swing strategy (holds days to
weeks, re-evaluated once per new daily bar close - not an intraday
strategy). Six rules exactly as specified by the source prompt this was
built from (see docs/sst_swing_spec.md for the full write-up and the
G-SST-1..7 gap decisions cited throughout this module):

  Rule 1 (risk)   -> size_for_risk
  Rule 2 (trend)  -> evaluate_sst_entry's SMA50 check
  Rule 3 (DMI)    -> latest_dmi_trigger
  Rule 4 (price confirmation, the actual entry signal) -> evaluate_sst_entry
  Rule 5 (initial stop) -> initial_stop_price
  Rule 6 (trailing stop / exit) -> trailing_stop_update

Same calling convention as src/orb.py's evaluate_orb_entry (so cycle.py
and src/backtest_engine.py can share one implementation): no data
fetching, no wall-clock "now" - the day being evaluated is whatever the
last row of `daily` is. Unlike ORB this needs no intraday bars at all -
every rule here operates on daily OHLC only.

Reuses src.technicals.adx_series for +DI/-DI (Wilder's standard smoothing,
already implemented and used elsewhere in this codebase - the source
prompt explicitly asked for "a standard, well-tested" implementation
rather than a hand-rolled one; this satisfies that without a new
dependency).
"""
from __future__ import annotations

import pandas as pd

from src.technicals import adx_series

HARD_MAX_RISK_PCT = 5.0  # Rule 1's own ceiling - enforced here regardless of what a strategy's config asks for


# ------------------------------------------------------- day classification ---
def classify_days(daily: pd.DataFrame) -> pd.DataFrame:
    """Adds is_inside / is_significant columns. An "inside day" extends
    neither the prior day's high nor its low; a "significant day" extends
    at least one side. The very first row has no prior day to compare
    against, so it's treated as significant by convention (nothing
    downstream ever needs to trail INTO it, only reference it as a
    starting point)."""
    out = daily.copy()
    prior_high = daily["High"].shift(1)
    prior_low = daily["Low"].shift(1)
    is_inside = (daily["High"] <= prior_high) & (daily["Low"] >= prior_low)
    out["is_inside"] = is_inside.fillna(False)
    out["is_significant"] = ~out["is_inside"]
    return out


def last_significant_extreme(days: pd.DataFrame, upto_pos: int, side: str) -> tuple[float, int] | None:
    """(extreme_price, its positional index) of the most recent
    significant day at or before `upto_pos` - inside days are skipped
    entirely, per the source prompt ("treat the sequence of significant
    days as if inside days weren't there"). side='long' -> that day's
    High (the level price must close above to confirm); 'short' -> Low."""
    col = "High" if side == "long" else "Low"
    sig = days["is_significant"].to_numpy()
    for pos in range(min(upto_pos, len(days) - 1), -1, -1):
        if sig[pos]:
            return float(days[col].iloc[pos]), pos
    return None


# ------------------------------------------------------------- DMI trigger ---
DEFAULT_DMI_TRIGGER_LOOKBACK_BARS = 10  # implementation detail, not one of the source prompt's own numbers -
                                        # see this function's own docstring for why a "last bar only" check is wrong


def latest_dmi_trigger(dmi: pd.DataFrame, touch_tolerance: float, diverge_confirm_bars: int,
                       lookback_bars: int = DEFAULT_DMI_TRIGGER_LOOKBACK_BARS) -> dict | None:
    """Scans BACKWARD up to `lookback_bars` days from the end of `dmi`
    (output of adx_series) for the most recent day a trigger fired -
    NOT necessarily the very last day. This matters: Rule 4's price
    confirmation can (and per max_entry_delay_bars, is expected to)
    happen up to a couple of bars AFTER the trigger day itself, so
    evaluate_sst_entry needs the trigger's own day, then searches forward
    from there for the breakout - a version of this function that only
    ever looked at the newest bar would make max_entry_delay_bars dead
    code (the trigger would always appear "fresh" or not at all).
    lookback_bars bounds how far back a trigger can still be considered
    live at all, independent of max_entry_delay_bars (which separately
    bounds how stale the BREAKOUT confirmation is allowed to be).

    Returns {"direction": "bullish"|"bearish", "trigger_idx": positional
    index of the day the trigger fired} or None. "bullish" = -DI
    crossing/touching down through +DI (Rule 3's long candidate);
    "bearish" is the mirror. Two ways a trigger fires (G-SST-1's
    tolerance/confirm-window decisions, since the source prompt names
    both mechanisms without numbers):
      (a) a clean sign-flip of (+DI - -DI) between two consecutive bars.
      (b) a "touch" (the two lines came within touch_tolerance of each
          other) followed by re-diverging past that tolerance within
          diverge_confirm_bars.
    """
    n = len(dmi)
    if n < 2:
        return None
    diff = dmi["plus_di"] - dmi["minus_di"]
    start = max(1, n - lookback_bars)

    for i in range(n - 1, start - 1, -1):
        prev, now = diff.iloc[i - 1], diff.iloc[i]
        if pd.notna(prev) and pd.notna(now):
            if prev <= 0 < now:
                return {"direction": "bullish", "trigger_idx": i}
            if prev >= 0 > now:
                return {"direction": "bearish", "trigger_idx": i}
        if pd.notna(now) and abs(now) > touch_tolerance:
            direction = "bullish" if now > 0 else "bearish"
            for back in range(1, diverge_confirm_bars + 1):
                j = i - back
                if j < 0:
                    break
                prior = diff.iloc[j]
                if pd.notna(prior) and abs(prior) <= touch_tolerance:
                    return {"direction": direction, "trigger_idx": i}
    return None


# --------------------------------------------------------------- entry (Rules 2-4) ---
def evaluate_sst_entry(daily: pd.DataFrame, rules: dict, side: str) -> dict:
    """Rules 2 (trend), 3 (DMI trigger), 4 (price confirmation) together.
    `daily` must already end at the day being evaluated (today's closed
    daily bar - this strategy only ever re-evaluates once per new bar,
    see G-SST-6). Returns {"pass": bool, "side": side, ...}; on pass also
    carries entry_price/initial_stop/significant_day_idx/trigger_idx -
    same result-shape convention as orb.evaluate_orb_entry."""
    sma_period = rules["sma_period"]
    dmi_period = rules["dmi_period"]
    need = max(sma_period, dmi_period, 200 if rules.get("avoid_200sma_obstruction", True) else 0) + 5
    if len(daily) < need:
        return {"pass": False, "side": side, "error": "insufficient daily history"}

    days = classify_days(daily)
    dmi = adx_series(daily["High"], daily["Low"], daily["Close"], period=dmi_period)
    sma = daily["Close"].rolling(sma_period).mean()

    last_close = float(daily["Close"].iloc[-1])
    last_sma = float(sma.iloc[-1])
    if pd.isna(last_sma) or last_sma == 0:
        return {"pass": False, "side": side, "error": "SMA not yet available"}

    band = rules["sma50_neutral_band_pct"] / 100.0
    if abs(last_close - last_sma) / last_sma <= band:
        return {"pass": False, "side": side, "reason": "price within SMA50 neutral band"}
    trend = "long" if last_close > last_sma else "short"
    if trend != side:
        return {"pass": False, "side": side, "reason": f"trend is {trend}, not {side}"}

    trig = latest_dmi_trigger(dmi, rules["dmi_touch_tolerance"], rules["dmi_diverge_confirm_bars"],
                              rules.get("dmi_trigger_lookback_bars", DEFAULT_DMI_TRIGGER_LOOKBACK_BARS))
    if trig is None:
        return {"pass": False, "side": side, "reason": "no DMI trigger"}
    want_direction = "bullish" if side == "long" else "bearish"
    if trig["direction"] != want_direction:
        return {"pass": False, "side": side, "reason": "DMI trigger direction mismatch"}

    # The reference significant day is anchored to the TRIGGER day, not
    # "as of today" - if today's own bar is itself significant (which it
    # usually is, right when it breaks out), searching as-of-today would
    # let a bar become its own reference level instead of confirming a
    # break of the PRIOR one. Rule 4 reads as "the most recent significant
    # day [as of when the trigger fired]", which is what this anchors to.
    extreme = last_significant_extreme(days, trig["trigger_idx"], side)
    if extreme is None:
        return {"pass": False, "side": side, "reason": "no significant day found"}
    level, sig_pos = extreme

    # G-SST-3: max_entry_delay_bars counts from the day price actually
    # confirmed the breakout, not the DMI trigger day - scan forward from
    # the trigger for the first bar that traded through `level`.
    break_pos = None
    for i in range(trig["trigger_idx"], len(days)):
        if side == "long" and float(days["High"].iloc[i]) > level:
            break_pos = i
            break
        if side == "short" and float(days["Low"].iloc[i]) < level:
            break_pos = i
            break
    if break_pos is None:
        return {"pass": False, "side": side, "reason": "no breakout confirmation yet"}
    delay = (len(days) - 1) - break_pos
    if delay > rules["max_entry_delay_bars"]:
        return {"pass": False, "side": side, "reason": f"breakout {delay} bars ago > max_entry_delay_bars"}

    entry_price = level
    stop = initial_stop_price(days, sig_pos, side, rules)

    if rules.get("avoid_200sma_obstruction", True):
        sma200 = daily["Close"].rolling(200).mean()
        sma200_val = sma200.iloc[-1]
        if pd.notna(sma200_val):
            risk = abs(entry_price - stop)
            lo, hi = (entry_price, entry_price + risk) if side == "long" else (entry_price - risk, entry_price)
            if lo <= float(sma200_val) <= hi:
                return {"pass": False, "side": side, "reason": "200SMA obstructs the first risk unit"}

    return {
        "pass": True, "side": side, "entry_price": round(entry_price, 4), "initial_stop": stop,
        "significant_day_idx": sig_pos, "trigger_idx": trig["trigger_idx"],
    }


# ------------------------------------------------------------------ Rule 5 ---
def initial_stop_price(days: pd.DataFrame, sig_pos: int, side: str, rules: dict) -> float:
    buf = rules["stop_buffer_pct"] / 100.0
    if side == "long":
        low = float(days["Low"].iloc[sig_pos])
        return round(low * (1 - buf), 4)
    high = float(days["High"].iloc[sig_pos])
    return round(high * (1 + buf), 4)


# ------------------------------------------------------------------ Rule 6 ---
def trailing_stop_update(daily: pd.DataFrame, side: str, current_stop: float, rules: dict) -> float | None:
    """Called once per new daily bar close for an open SST position.
    Returns a NEW stop price if this bar's significant-day extreme moves
    the stop in the trade's favor, else None. Inside days never move the
    stop. Big-move-day exception: a day whose own range is
    >= big_move_threshold_pct of price uses the midpoint of that range as
    the reference instead of the full extreme (locks in more of an
    outsized move). Hard "never widen" enforcement: a candidate that
    would move the stop AWAY from price is simply not returned."""
    days = classify_days(daily)
    if len(days) < 2:
        return None
    last = days.iloc[-1]
    if bool(last["is_inside"]) or not bool(last["is_significant"]):
        return None

    buf = rules["stop_buffer_pct"] / 100.0
    day_range = float(last["High"] - last["Low"])
    price_ref = float(last["Close"]) or 1.0
    big_move = day_range >= (rules["big_move_threshold_pct"] / 100.0) * price_ref

    if side == "long":
        if float(last["High"]) <= float(days["High"].iloc[-2]):
            return None  # not a new high in our favor
        ref = float(last["Low"]) + day_range * 0.5 if big_move else float(last["Low"])
        new_stop = round(ref * (1 - buf), 4)
        return new_stop if new_stop > current_stop else None
    else:
        if float(last["Low"]) >= float(days["Low"].iloc[-2]):
            return None  # not a new low in our favor
        ref = float(last["High"]) - day_range * 0.5 if big_move else float(last["High"])
        new_stop = round(ref * (1 + buf), 4)
        return new_stop if new_stop < current_stop else None


# --------------------------------------------------------------- position sizing ---
def size_for_risk(equity: float, entry_price: float, stop_price: float, rules: dict) -> tuple[int, str | None]:
    """(shares, skip_reason). Rule 1: risk = (entry-stop) x shares must be
    <= equity x max_risk_pct, hard-ceilinged at HARD_MAX_RISK_PCT
    regardless of what rules asks for. shares=0 (with a reason) if the
    stop is too far away relative to account size - caller logs and skips,
    per the source prompt's own instruction."""
    max_risk_pct = min(float(rules.get("max_risk_pct", 3.0)), HARD_MAX_RISK_PCT)
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0:
        return 0, "invalid risk_per_share (entry == stop)"
    if equity <= 0:
        return 0, "invalid equity"
    max_risk_amount = equity * (max_risk_pct / 100.0)
    shares = int(max_risk_amount // risk_per_share)  # whole shares only - no lot-size complexity for US equities
    if shares <= 0:
        return 0, "stop too far for account size (0 shares)"
    return shares, None
