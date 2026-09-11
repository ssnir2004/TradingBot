"""Phase 2 - PnL report over the signals momentum.backfill already
collected. Not a new data-gathering pass: every signal in momentum_signals
(mode='backfill') already carries its own entry/stop/scale levels (G7-G12,
computed at fire time); this module just replays each one forward through
real historical 5-minute bars, applying the Shared Exit Engine exactly as
documented, and reports the result.

Simulation rules per signal (all from docs/momentum_strategy_spec.md's
G7-G12, no new thresholds invented here):
  - fill assumed at entry_ref (no slippage modeled - see caveats)
  - stop at stop_price; max_loss_price as a second, wider backstop (catches
    a gap through the stop bar)
  - first_scale_r: sell 50% there, move stop to breakeven
  - second_scale_r: sell another 25% (of the original) there
  - the final 25% "runner" trails the prior 5m candle's low
  - red_candle_exit: a red 5m candle closes and the position is below
    red_candle_hold_min_r -> exit the remainder
  - bailout: bailout_minutes elapse with no real move and no scale yet ->
    exit the remainder
  - anything still open at the end of the RTH session closes at the last
    print ("eod")

Caveats (this is still a "poor backtest", not a fill simulator):
  - no slippage/partial fills; a breakout signal is assumed filled exactly
    at its trigger level, which real order flow on a low-float mover often
    won't match.
  - position sizing is illustrative (a nominal account size), since phase 1
    signals were generated with equity_usd=None (see momentum.backfill).
  - 5-minute bar resolution only - an intraday stop/target touch inside a
    bar is assumed fillable at that exact price, not whatever the real
    tick sequence within the bar would have given.
"""
from __future__ import annotations

import json
import logging
from datetime import time as dtime

import pandas as pd

from momentum import store
from momentum.backfill import fetch_intraday_day
from momentum.config import load_config

log = logging.getLogger("momentum.backtest")

NOMINAL_EQUITY_USD = 12_000.0   # illustrative only - see module docstring
RTH_START, RTH_END = dtime(9, 30), dtime(16, 0)


def _rth_bars(day_bars: pd.DataFrame) -> pd.DataFrame:
    times = day_bars.index.time
    return day_bars[(times >= RTH_START) & (times <= RTH_END)]


def simulate_signal(sig: dict, day_bars_rth: pd.DataFrame, cfg: dict) -> dict:
    ex = cfg["exit"]
    entry_ref, stop, max_loss = sig["entry_ref"], sig["stop_price"], sig["max_loss_price"]
    scale1, scale2 = sig["first_scale_price"], sig["second_scale_price"]
    r_unit = entry_ref - stop
    if r_unit <= 0:
        return {"outcome": "invalid", "r_multiple": 0.0, "exit_reason": "bad_levels", "bars_held": 0}

    try:
        signal_ts = pd.Timestamp(sig["signal_iso"])
    except (ValueError, TypeError):
        return {"outcome": "invalid", "r_multiple": 0.0, "exit_reason": "bad_timestamp", "bars_held": 0}

    if day_bars_rth.index.tz is not None and signal_ts.tz is None:
        signal_ts = signal_ts.tz_localize(day_bars_rth.index.tz)
    future = day_bars_rth[day_bars_rth.index > signal_ts]
    if future.empty:
        return {"outcome": "no_data", "r_multiple": 0.0, "exit_reason": "no_data", "bars_held": 0}

    remaining = 1.0
    realized_r = 0.0
    cur_stop = stop
    scaled1 = scaled2 = False
    exit_reason = None
    last_close = entry_ref
    bars_elapsed = 0

    for i in range(len(future)):
        bar = future.iloc[i]
        bars_elapsed += 1
        last_close = float(bar["Close"])
        low, high = float(bar["Low"]), float(bar["High"])

        if low <= cur_stop:
            realized_r += remaining * ((cur_stop - entry_ref) / r_unit)
            remaining = 0.0
            exit_reason = "breakeven_stop" if cur_stop >= entry_ref - 1e-9 else "stop_loss"
            break
        if low <= max_loss:
            realized_r += remaining * ((max_loss - entry_ref) / r_unit)
            remaining = 0.0
            exit_reason = "max_loss"
            break

        if not scaled1 and high >= scale1:
            realized_r += 0.5 * ex["first_scale_r"]
            remaining -= 0.5
            scaled1 = True
            if ex["breakeven_after_first_scale"]:
                cur_stop = max(cur_stop, entry_ref)
        if scaled1 and not scaled2 and high >= scale2:
            realized_r += 0.25 * ex["second_scale_r"]
            remaining -= 0.25
            scaled2 = True
        if remaining <= 1e-9:
            exit_reason = "fully_scaled"
            break

        if scaled1 and ex["runner_trail"] == "prior_5m_low" and i >= 1:
            cur_stop = max(cur_stop, float(future.iloc[i - 1]["Low"]))

        cur_r = (last_close - entry_ref) / r_unit
        if ex["red_candle_exit_enabled"] and float(bar["Close"]) < float(bar["Open"]) and cur_r < ex["red_candle_hold_min_r"]:
            realized_r += remaining * cur_r
            remaining = 0.0
            exit_reason = "red_candle"
            break
        if not scaled1 and bars_elapsed * 5 >= ex["bailout_minutes"] and cur_r <= 0.1:
            realized_r += remaining * cur_r
            remaining = 0.0
            exit_reason = "bailout"
            break

    if remaining > 1e-9:
        cur_r = (last_close - entry_ref) / r_unit
        realized_r += remaining * cur_r
        exit_reason = exit_reason or "eod"

    outcome = "win" if realized_r > 0.05 else ("loss" if realized_r < -0.05 else "scratch")
    risk_usd = NOMINAL_EQUITY_USD * (cfg["risk"]["per_trade_pct"] / 100.0)
    return {
        "outcome": outcome, "r_multiple": round(realized_r, 3), "exit_reason": exit_reason,
        "bars_held": bars_elapsed, "scaled1": scaled1, "scaled2": scaled2,
        "dollars_nominal": round(risk_usd * realized_r, 2),
    }


def run_backtest_report(mode: str = "backfill") -> dict:
    cfg = load_config()
    signals = [s for s in store.recent_signals(limit=5000) if s["mode"] == mode]
    log.info("simulating %s signals", len(signals))

    results = []
    day_cache: dict[tuple[str, str], pd.DataFrame | None] = {}
    for i, sig in enumerate(signals):
        key = (sig["symbol"], sig["trade_date"])
        if key not in day_cache:
            try:
                day = pd.Timestamp(sig["trade_date"]).date()
            except ValueError:
                day_cache[key] = None
            else:
                raw = fetch_intraday_day(sig["symbol"], day)
                day_cache[key] = _rth_bars(raw) if raw is not None else None
        day_bars = day_cache[key]
        if day_bars is None or day_bars.empty:
            outcome = {"outcome": "no_data", "r_multiple": 0.0, "exit_reason": "no_data", "bars_held": 0}
        else:
            outcome = simulate_signal(sig, day_bars, cfg)
        outcome["signal_id"] = sig["id"]
        outcome["strategy"] = sig["strategy"]
        outcome["symbol"] = sig["symbol"]
        outcome["trade_date"] = sig["trade_date"]
        outcome["conviction"] = sig["conviction"]
        results.append(outcome)
        store.set_signal_outcome(sig["id"], outcome["outcome"], outcome)
        if i % 25 == 0:
            log.info("simulated %s/%s", i, len(signals))

    return _summarize(results)


def summary_from_stored(mode: str = "backfill") -> dict:
    """Rebuilds the same summary shape as run_backtest_report(), but from
    each signal's already-stored outcome_json (set_signal_outcome) instead
    of re-simulating - the fast path the dashboard's /api/momentum/backtest
    endpoint uses, since re-fetching bars for every request would be slow
    and pointless (the outcome doesn't change between page loads)."""
    rows = [s for s in store.recent_signals(limit=5000) if s["mode"] == mode and s.get("outcome_json")]
    results = []
    for s in rows:
        try:
            detail = json.loads(s["outcome_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        detail["strategy"] = s["strategy"]
        detail["symbol"] = s["symbol"]
        detail["trade_date"] = s["trade_date"]
        detail["conviction"] = s["conviction"]
        detail.setdefault("outcome", s.get("outcome"))
        results.append(detail)
    return _summarize(results)


def _summarize(results: list[dict]) -> dict:
    def agg(rows: list[dict]) -> dict:
        rows = [r for r in rows if r["outcome"] != "no_data"]
        n = len(rows)
        if n == 0:
            return {"n": 0}
        wins = [r for r in rows if r["outcome"] == "win"]
        losses = [r for r in rows if r["outcome"] == "loss"]
        total_r = sum(r["r_multiple"] for r in rows)
        gross_win = sum(r["r_multiple"] for r in wins)
        gross_loss = -sum(r["r_multiple"] for r in losses)
        return {
            "n": n,
            "wins": len(wins), "losses": len(losses), "scratches": n - len(wins) - len(losses),
            "win_rate_pct": round(100 * len(wins) / n, 1),
            "avg_r": round(total_r / n, 3),
            "total_r": round(total_r, 2),
            "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
            "total_dollars_nominal": round(sum(r["dollars_nominal"] for r in rows), 2),
            "exit_reasons": _counts([r["exit_reason"] for r in rows]),
        }

    def _counts(vals):
        out: dict[str, int] = {}
        for v in vals:
            out[v] = out.get(v, 0) + 1
        return out

    by_strategy = {}
    for strat in sorted({r["strategy"] for r in results}):
        by_strategy[strat] = agg([r for r in results if r["strategy"] == strat])

    ranked = sorted([r for r in results if r["outcome"] != "no_data"], key=lambda r: r["r_multiple"])
    return {
        "overall": agg(results),
        "by_strategy": by_strategy,
        "no_data_count": sum(1 for r in results if r["outcome"] == "no_data"),
        "worst_10": ranked[:10],
        "best_10": list(reversed(ranked[-10:])),
    }
