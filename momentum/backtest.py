"""Phase 2 - PnL report over the signals momentum.backfill already
collected. Not a new data-gathering pass: every signal in momentum_signals
(mode='backfill') already carries its own entry/stop/scale levels (G7-G12,
computed at fire time); this module just replays each one forward through
real historical 5-minute bars, applying the Shared Exit Engine exactly as
documented, and reports the result - both an optimistic ("gross") figure
and a realistic one net of two real-world costs, side by side, so neither
number gets mistaken for the other.

Simulation mechanics (all from docs/momentum_strategy_spec.md's G7-G12, no
new entry/exit thresholds invented here):
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

Two real-world costs, both config-driven (momentum.config's "execution"
and "commissions" blocks), added 2026-09-11 after the first version's
headline numbers (69.7% win, +0.82R avg, PF 4.7) turned out to be fragile:

  - **Slippage** (execution.slippage_cents, default 3): the assumed fill is
    `entry_ref + slippage_cents` - worse than the technical trigger price -
    while stop/scale prices stay at their ORIGINAL fixed levels (a resting
    order at a technical price doesn't move just because your own fill
    was worse). This widens the real risk unit and shrinks the effective
    reward, exactly how slippage bites in practice.
  - **Commissions** (the broker's own schedule: a flat fee up to N shares,
    per-share above it - both configurable). Every leg of a trade
    (the entry buy, plus one sell per scale-out, plus the final exit)
    is its own order and its own commission - a 4-leg trade (both
    scale-outs + a runner) can rack up several times the single-order
    minimum, which matters a lot on the size of position a $60 risk
    budget produces.

Nothing here changes the LIVE scanner - slippage/commissions only affect
this backtest's reported PnL, never the entry/exit signal itself.

Remaining caveats (this is still a "poor backtest", not a fill simulator):
  - position sizing is illustrative (a nominal account size), since phase 1
    signals were generated with equity_usd=None (see momentum.backfill).
  - 5-minute bar resolution only - an intraday stop/target touch inside a
    bar is assumed fillable at that exact price, not whatever the real
    tick sequence within the bar would have given.
  - slippage is a single flat assumption, not a function of the specific
    stock's liquidity/float/spread that day.
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


def _commission(shares: int, cfg: dict) -> float:
    c = cfg["commissions"]
    if not c["enabled"] or shares <= 0:
        return 0.0
    if shares <= c["flat_fee_max_shares"]:
        return c["flat_fee_usd"]
    return round(shares * c["per_share_usd"], 2)


def _nominal_shares(entry_ref: float, r_unit: float, cfg: dict) -> tuple[int, float]:
    """(shares, risk_usd) at NOMINAL_EQUITY_USD and the configured
    per-trade risk % / notional cap - the same formula Signal.finalize
    uses for a real account, just against an illustrative one (phase 1
    signals were generated with equity_usd=None)."""
    risk = cfg["risk"]
    risk_usd = NOMINAL_EQUITY_USD * (risk["per_trade_pct"] / 100.0)
    if r_unit <= 0:
        return 0, risk_usd
    shares = int(risk_usd / r_unit)
    notional_cap = int(risk["max_position_notional_usd"] / max(entry_ref, 0.01))
    return max(0, min(shares, notional_cap)), risk_usd


def simulate_signal(sig: dict, day_bars_rth: pd.DataFrame, cfg: dict) -> dict:
    ex = cfg["exit"]
    entry_ref, stop, max_loss = sig["entry_ref"], sig["stop_price"], sig["max_loss_price"]
    scale1, scale2 = sig["first_scale_price"], sig["second_scale_price"]
    slippage = cfg["execution"]["slippage_cents"] / 100.0

    r_unit_gross = entry_ref - stop
    if r_unit_gross <= 0:
        return {"outcome": "invalid", "r_multiple_gross": 0.0, "r_multiple_net": 0.0,
                "exit_reason": "bad_levels", "bars_held": 0}

    try:
        signal_ts = pd.Timestamp(sig["signal_iso"])
    except (ValueError, TypeError):
        return {"outcome": "invalid", "r_multiple_gross": 0.0, "r_multiple_net": 0.0,
                "exit_reason": "bad_timestamp", "bars_held": 0}

    if day_bars_rth.index.tz is not None and signal_ts.tz is None:
        signal_ts = signal_ts.tz_localize(day_bars_rth.index.tz)
    future = day_bars_rth[day_bars_rth.index > signal_ts]
    if future.empty:
        return {"outcome": "no_data", "r_multiple_gross": 0.0, "r_multiple_net": 0.0,
                "exit_reason": "no_data", "bars_held": 0}

    # fill = the realistic (worse) cost basis; the technical stop/scale
    # prices are unaffected - see module docstring
    fill = entry_ref + slippage
    r_unit_net = fill - stop
    if r_unit_net <= 0:
        r_unit_net = r_unit_gross  # degenerate (slippage >= stop distance) - fall back rather than divide by ~0

    remaining = 1.0
    realized_r_gross = realized_r_net = 0.0
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
            realized_r_gross += remaining * ((cur_stop - entry_ref) / r_unit_gross)
            realized_r_net += remaining * ((cur_stop - fill) / r_unit_net)
            remaining = 0.0
            exit_reason = "breakeven_stop" if cur_stop >= entry_ref - 1e-9 else "stop_loss"
            break
        if low <= max_loss:
            realized_r_gross += remaining * ((max_loss - entry_ref) / r_unit_gross)
            realized_r_net += remaining * ((max_loss - fill) / r_unit_net)
            remaining = 0.0
            exit_reason = "max_loss"
            break

        if not scaled1 and high >= scale1:
            realized_r_gross += 0.5 * ex["first_scale_r"]
            realized_r_net += 0.5 * ((scale1 - fill) / r_unit_net)
            remaining -= 0.5
            scaled1 = True
            if ex["breakeven_after_first_scale"]:
                cur_stop = max(cur_stop, entry_ref)
        if scaled1 and not scaled2 and high >= scale2:
            realized_r_gross += 0.25 * ex["second_scale_r"]
            realized_r_net += 0.25 * ((scale2 - fill) / r_unit_net)
            remaining -= 0.25
            scaled2 = True
        if remaining <= 1e-9:
            exit_reason = "fully_scaled"
            break

        if scaled1 and ex["runner_trail"] == "prior_5m_low" and i >= 1:
            cur_stop = max(cur_stop, float(future.iloc[i - 1]["Low"]))

        cur_r_gross = (last_close - entry_ref) / r_unit_gross
        cur_r_net = (last_close - fill) / r_unit_net
        if ex["red_candle_exit_enabled"] and float(bar["Close"]) < float(bar["Open"]) and cur_r_gross < ex["red_candle_hold_min_r"]:
            realized_r_gross += remaining * cur_r_gross
            realized_r_net += remaining * cur_r_net
            remaining = 0.0
            exit_reason = "red_candle"
            break
        if not scaled1 and bars_elapsed * 5 >= ex["bailout_minutes"] and cur_r_gross <= 0.1:
            realized_r_gross += remaining * cur_r_gross
            realized_r_net += remaining * cur_r_net
            remaining = 0.0
            exit_reason = "bailout"
            break

    if remaining > 1e-9:
        realized_r_gross += remaining * ((last_close - entry_ref) / r_unit_gross)
        realized_r_net += remaining * ((last_close - fill) / r_unit_net)
        exit_reason = exit_reason or "eod"

    shares, risk_usd = _nominal_shares(entry_ref, r_unit_gross, cfg)
    legs = [shares]
    if scaled1 and scaled2:
        legs += [round(shares * 0.5), round(shares * 0.25), round(shares * 0.25)]
    elif scaled1:
        legs += [round(shares * 0.5), round(shares * 0.5)]
    else:
        legs += [shares]
    commission_usd = round(sum(_commission(sh, cfg) for sh in legs), 2)

    dollars_gross = round(risk_usd * realized_r_gross, 2)
    dollars_net = round(risk_usd * realized_r_net - commission_usd, 2)
    r_multiple_net_after_commission = dollars_net / risk_usd if risk_usd else 0.0

    outcome = ("win" if r_multiple_net_after_commission > 0.05
              else ("loss" if r_multiple_net_after_commission < -0.05 else "scratch"))

    return {
        "outcome": outcome, "exit_reason": exit_reason, "bars_held": bars_elapsed,
        "scaled1": scaled1, "scaled2": scaled2,
        "r_multiple_gross": round(realized_r_gross, 3),
        "r_multiple_net": round(r_multiple_net_after_commission, 3),
        "shares": shares, "commission_usd": commission_usd,
        "dollars_gross": dollars_gross, "dollars_net": dollars_net,
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
            outcome = {"outcome": "no_data", "r_multiple_gross": 0.0, "r_multiple_net": 0.0,
                      "exit_reason": "no_data", "bars_held": 0}
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
    of re-simulating - the fast path the dashboard's /api/momentum/
    backtest_summary endpoint uses, since re-fetching bars for every
    request would be slow and pointless (the outcome doesn't change
    between page loads)."""
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
        rows = [r for r in rows if r["outcome"] not in ("no_data", "invalid")]
        n = len(rows)
        if n == 0:
            return {"n": 0}
        wins = [r for r in rows if r["outcome"] == "win"]
        losses = [r for r in rows if r["outcome"] == "loss"]

        def side(field: str) -> dict:
            total = sum(r[field] for r in rows)
            gross_win = sum(r[field] for r in rows if r[field] > 0)
            gross_loss = -sum(r[field] for r in rows if r[field] < 0)
            return {
                "avg_r": round(total / n, 3),
                "total_r": round(total, 2),
                "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
            }

        return {
            "n": n,
            "wins": len(wins), "losses": len(losses), "scratches": n - len(wins) - len(losses),
            "win_rate_pct": round(100 * len(wins) / n, 1),
            **{f"{k}_gross": v for k, v in side("r_multiple_gross").items()},
            **{f"{k}_net": v for k, v in side("r_multiple_net").items()},
            "total_dollars_gross": round(sum(r.get("dollars_gross", 0.0) for r in rows), 2),
            "total_dollars_net": round(sum(r.get("dollars_net", 0.0) for r in rows), 2),
            "total_commission": round(sum(r.get("commission_usd", 0.0) for r in rows), 2),
            "exit_reasons": _counts([r["exit_reason"] for r in rows]),
            # backward/forward-compatible aliases some older callers expect
            "avg_r": round(sum(r["r_multiple_net"] for r in rows) / n, 3),
            "total_r": round(sum(r["r_multiple_net"] for r in rows), 2),
            "profit_factor": side("r_multiple_net")["profit_factor"],
            "total_dollars_nominal": round(sum(r.get("dollars_net", 0.0) for r in rows), 2),
        }

    def _counts(vals):
        out: dict[str, int] = {}
        for v in vals:
            out[v] = out.get(v, 0) + 1
        return out

    by_strategy = {}
    for strat in sorted({r["strategy"] for r in results}):
        by_strategy[strat] = agg([r for r in results if r["strategy"] == strat])

    ranked = sorted(
        [r for r in results if r["outcome"] not in ("no_data", "invalid")],
        key=lambda r: r["r_multiple_net"],
    )
    return {
        "overall": agg(results),
        "by_strategy": by_strategy,
        "no_data_count": sum(1 for r in results if r["outcome"] in ("no_data", "invalid")),
        "worst_10": ranked[:10],
        "best_10": list(reversed(ranked[-10:])),
    }
