"""Read-only observer layer for the "Bot Decision Intelligence Center"
dashboard (web/app.py's /api/decision_center, web/templates/decision_
center.html). Every function here is PURE OBSERVATION: it reads already-
open bot-tracked positions (db.get_open_positions), the currently active
strategy for that position's side (db.get_active_rules), the account's own
decision_log (db.get_decision_log_for_symbol), and fresh market data
(yfinance, via cycle.get_chart_bars - no IBKR connection needed, same as
cycle._evaluate_entry_filters/_evaluate_orb_entry) - it never writes to
db.positions, never places or cancels an order, and never feeds anything
back into cycle.manage_position's real decisions. If this whole module
raised an exception on every call, live trading would be completely
unaffected - see cycle.manage_position for the actual, live decision
logic this module only ever narrates after the fact.

The "dynamic recovery" (V10) / "dynamic risk reduction" (V8/V9) replay
below is the one subtle piece worth flagging up front: those two
mechanisms are REAL code (src/backtest_engine.py's own _evaluate_v10_
recovery/_evaluate_v6_risk_event), reused here verbatim against fresh
live intraday bars so a V8/V9/V10 position gets a genuine, non-fabricated
signal readout - but they are NOT wired into cycle.manage_position at
all (confirmed by reading cycle.py: manage_position's "no_stop_delayed_
trail" branch only ever does the base hard-stop/trailing state machine,
same as v4.1/v4.2/v4.3). This replay's own hard_stop_price is therefore
computed on a throwaway copy of the position, purely for display - it is
NEVER written back to db.positions and never controls the real resting
broker stop. Every response this module builds carries "live_enforced":
False for this reason, and the dashboard must show that plainly rather
than implying the bot is actually acting on it.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

import cycle
from src import backtest_engine as bt
from src import db, orb


# ------------------------------------------------------------- fetching ---
def fetch_bars(symbol: str) -> pd.DataFrame | None:
    """5-minute bars, tz-converted to ET, going back far enough to cover a
    same-day-entry position's whole life plus (rare) an overnight hold -
    reuses cycle.get_chart_bars exactly as the dashboard's own candlestick
    chart already does, so this needs no separate yfinance wiring. Exposed
    unprefixed - web/app.py's own Historical Replay endpoint reuses this
    directly for a closed trade's own price path."""
    return cycle.get_chart_bars(symbol, "5m")


def _parse_entry_ts(pos: dict) -> pd.Timestamp | None:
    try:
        return pd.Timestamp(pos["entry_time_iso"])
    except (ValueError, TypeError, KeyError):
        return None


# ------------------------------------------------------ generic snapshot ---
def position_snapshot(pos: dict, exit_cfg: dict) -> dict:
    """The Overview tab's per-position real fields - entry/current/R/MFE/
    MAE/age/active stop/exit-logic description/status badge. Every value
    here already exists on `pos` (as upserted by cycle.manage_position) or
    is a cheap derived read (age, exit-logic text) - nothing new is
    computed against the market."""
    side = pos.get("side", "long")
    entry = pos["entry_price"]
    price = cycle._current_price(pos["symbol"])
    initial_risk = (pos["initial_stop"] - entry) if side == "short" else (entry - pos["initial_stop"])
    r_multiple = pos.get("r_multiple")
    if price is not None and initial_risk and initial_risk > 0:
        r_multiple = ((entry - price) if side == "short" else (price - entry)) / initial_risk

    mfe_price, mae_price = pos.get("mfe_price"), pos.get("mae_price")
    mfe_r = (((mfe_price - entry) if side == "long" else (entry - mfe_price)) / initial_risk) if (mfe_price is not None and initial_risk) else None
    mae_r = (((entry - mae_price) if side == "long" else (mae_price - entry)) / initial_risk) if (mae_price is not None and initial_risk) else None

    try:
        entry_ts = datetime.fromisoformat(pos["entry_time_iso"])
        age_minutes = (datetime.now(entry_ts.tzinfo) - entry_ts).total_seconds() / 60
    except (ValueError, TypeError, KeyError):
        age_minutes = None

    style = exit_cfg.get("management_style")
    active_stop = pos.get("stop_price")
    if style == "no_stop_delayed_trail":
        exit_logic = (
            f"No breakeven stage - holds under its real hard stop"
            + (f" ({exit_cfg['hard_stop_R']}R)" if exit_cfg.get("hard_stop_R") is not None else " (tight initial stop, no hard_stop_R configured)")
            + f", switches to trailing once MFE clears {exit_cfg.get('trailing_trigger_R', 1.20)}R."
        )
    elif style == "staged_trail":
        exit_logic = f"Breakeven at {exit_cfg.get('breakeven_trigger_R', 2.0)}R, trailing starts at {exit_cfg.get('trailing_trigger_R', 3.0)}R."
    elif style == "fixed_target_no_trail":
        exit_logic = f"Fixed target only, no trailing - closes whole position at target (${pos.get('target_price'):.2f})." if pos.get("target_price") else "Fixed target only, no trailing."
    else:
        exit_logic = f"Breakeven at {exit_cfg.get('breakeven_trigger_R', '?')}R, then swing-pivot trailing."

    if pos.get("trail_activated"):
        status = "TRAILING"
    elif r_multiple is not None and r_multiple <= -0.75:
        status = "AT_RISK"
    elif r_multiple is not None and r_multiple >= (exit_cfg.get("trailing_trigger_R") or 1.0) * 0.6:
        status = "PROGRESSING"
    else:
        status = "HOLDING"

    return {
        "symbol": pos["symbol"], "side": side, "management_style": style,
        "entry_price": entry, "current_price": price, "r_multiple": r_multiple,
        "mfe_price": mfe_price, "mfe_r": mfe_r, "mae_price": mae_price, "mae_r": mae_r,
        "age_minutes": age_minutes, "qty": pos.get("qty"),
        "initial_stop": pos.get("initial_stop"), "hard_stop_price": pos.get("hard_stop_price"),
        "active_stop": active_stop, "trail_activated": bool(pos.get("trail_activated")),
        "trail_activated_at_r": pos.get("trail_activated_at_r"),
        "exit_logic": exit_logic, "status": status,
    }


# ------------------------------------------------------ attractiveness ---
def attractiveness_score(snapshot: dict, exit_cfg: dict) -> dict:
    """A transparent 0-100 score built ONLY from fields position_snapshot
    already computed - every contributor below is a plain, documented
    arithmetic transform of a real value (R-multiple progress toward this
    strategy's own trailing trigger, how much of the best excursion is
    still retained, whether trailing has locked in gains, and how close
    price sits to the hard stop), never a fabricated or opaque number.
    Returned as {"score", "contributors": [...]} so the dashboard can show
    the breakdown, not just the total."""
    r = snapshot.get("r_multiple")
    mfe_r = snapshot.get("mfe_r")
    trigger_r = exit_cfg.get("trailing_trigger_R") or 1.2
    contributors = []

    base = 50.0
    contributors.append({"label": "Base", "points": base, "detail": "Neutral starting point"})

    if r is not None:
        progress_pts = max(-25.0, min(25.0, (r / trigger_r) * 25.0))
        contributors.append({
            "label": "R-multiple progress", "points": round(progress_pts, 1),
            "detail": f"{r:.2f}R of this strategy's {trigger_r}R trailing trigger",
        })
    else:
        progress_pts = 0.0

    if mfe_r is not None and mfe_r > 0 and r is not None:
        retention = max(0.0, min(1.0, r / mfe_r)) if mfe_r > 0 else 0.0
        retention_pts = retention * 15.0
        contributors.append({
            "label": "MFE retention", "points": round(retention_pts, 1),
            "detail": f"Holding {retention * 100:.0f}% of its best excursion ({mfe_r:.2f}R)",
        })
    else:
        retention_pts = 0.0

    trail_pts = 10.0 if snapshot.get("trail_activated") else 0.0
    if snapshot.get("trail_activated"):
        contributors.append({"label": "Trailing active", "points": trail_pts, "detail": "Gains are locked in behind a trailing stop"})

    penalty_pts = 0.0
    if r is not None and r < 0:
        penalty_pts = -min(20.0, abs(r) * 20.0)
        contributors.append({"label": "Adverse move", "points": round(penalty_pts, 1), "detail": f"{r:.2f}R against entry"})

    score = max(0.0, min(100.0, base + progress_pts + retention_pts + trail_pts + penalty_pts))
    return {"score": round(score, 1), "contributors": contributors}


# --------------------------------------------------------- entry detail ---
def entry_qualified_checklist(account_id: int, mode: str, pos: dict) -> dict:
    """The "Entry Qualified" checklist (Decision Engine tab) - built from
    the REAL filter_eval/orb_filter_eval decision_log row cycle.
    _evaluate_entry_filters/_evaluate_orb_entry already wrote at scan time
    for this exact symbol+side (see those functions' own log_decision
    calls) - never recomputed or guessed after the fact. None of the
    fields here are synthesized: whatever detail dict the live scan
    actually evaluated (D1-D3/I1-I3, or the ORB confluence/retest fields)
    is what's shown, verbatim, closest in time to (at or before) this
    position's own entry."""
    events = db.get_decision_log_for_symbol(account_id, mode, pos["symbol"])
    entry_ts = pos.get("entry_time_iso") or ""
    candidates = [
        e for e in events
        if e["event"] in ("filter_eval", "orb_filter_eval") and e["timestamp_iso"] <= entry_ts
    ]
    if not candidates:
        return {"available": False, "reason": "No filter_eval scan logged for this symbol before entry (position may predate this logging, or was opened manually)."}
    detail = candidates[-1]["payload"]
    return {"available": True, "at": candidates[-1]["timestamp_iso"], "model": "orb" if "or_high" in detail else "classic", "detail": detail}


def position_timeline(account_id: int, mode: str, symbol: str) -> list[dict]:
    """Every decision_log row for this symbol, oldest first - the Timeline
    tab's raw feed (entry, breakeven/trail/hard-stop-backfill events,
    the eventual stop_out/target_close/force_close, plus every filter_eval
    scan) - db.get_decision_log_for_symbol already does the real work."""
    events = db.get_decision_log_for_symbol(account_id, mode, symbol)
    return [{"id": e["id"], "at": e["timestamp_iso"], "event": e["event"], "detail": e["payload"]} for e in events]


# --------------------------------------------------- V8/V9/V10 replay ---
def _entry_window_features(bars: pd.DataFrame, entry_ts: pd.Timestamp, cfg: dict):
    window = bars[bars.index <= entry_ts]["Close"].iloc[-orb._CONFLUENCE_LOOKBACK_BARS:]
    if window.empty:
        return None, None
    rsi_series = orb._compute_rsi_series(window, cfg.get("rsi_period", 14))
    ema_series = orb._compute_ema_series(window, cfg.get("ema9_period", cfg.get("ema_period", 9)))
    rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]) else None
    ema9 = float(ema_series.iloc[-1]) if not ema_series.empty and not pd.isna(ema_series.iloc[-1]) else None
    return rsi, ema9


def replay_dynamic_signals(pos: dict, rules: dict) -> dict:
    """Genuinely re-runs backtest_engine.py's own V8/V9 ("dynamic_risk_
    reduction") or V10 ("dynamic_recovery") per-bar state machine against
    fresh live intraday bars, on a throwaway copy of this position -
    never touching db.positions or the real pos dict cycle.manage_
    position operates on. See this module's own docstring for why
    "live_enforced" is always False here: neither mechanism is actually
    wired into cycle.manage_position today, so this is a real, honest
    computation of what these functions WOULD decide, shown for
    awareness/research, not a report of something the bot itself acted
    on."""
    exit_cfg = rules.get("exit", {})
    recovery_cfg = exit_cfg.get("dynamic_recovery")
    dynamic_cfg = exit_cfg.get("dynamic_risk_reduction")
    if recovery_cfg is None and dynamic_cfg is None:
        return {"mechanism": None, "applicable": False}

    mechanism = "v10_dynamic_recovery" if recovery_cfg is not None else "v8v9_dynamic_risk_reduction"
    side = pos.get("side", "long")
    entry_ts = _parse_entry_ts(pos)
    bars = fetch_bars(pos["symbol"])
    if bars is None or bars.empty or entry_ts is None:
        return {"mechanism": mechanism, "applicable": True, "live_enforced": False, "error": "No intraday data available for replay."}

    cfg = recovery_cfg if recovery_cfg is not None else dynamic_cfg
    entry_rsi, entry_ema9 = _entry_window_features(bars, entry_ts, cfg)
    today_bars = bars[bars.index.date == entry_ts.date()]
    or_range = orb.compute_opening_range(today_bars) if not today_bars.empty else None
    entry_price = pos["entry_price"]

    rpos = {
        "side": side, "entry_price": entry_price, "initial_stop": pos["initial_stop"],
        "mfe_price": entry_price, "mae_price": entry_price,
        "hard_stop_price": pos.get("hard_stop_price"), "trail_activated": False,
    }
    initial_risk = (rpos["initial_stop"] - entry_price) if side == "short" else (entry_price - rpos["initial_stop"])
    trailing_trigger_r = exit_cfg.get("trailing_trigger_R", 1.20)

    common = {
        "cfg": cfg, "entry_ts": entry_ts,
        "or_high": or_range["or_high"] if or_range else None, "or_low": or_range["or_low"] if or_range else None,
        "entry_rsi": entry_rsi, "entry_above_ema9": (entry_price > entry_ema9) if entry_ema9 is not None else None,
        "initial_hard_stop_r": exit_cfg.get("hard_stop_R"),
    }
    if recovery_cfg is not None:
        rpos["dynamic_recovery"] = {
            **common,
            "checkpoints": recovery_cfg.get("checkpoint_offsets_minutes", list(bt._V10_CHECKPOINT_OFFSETS_MINUTES)),
            "next_checkpoint_idx": 0, "state": bt._V10_STATE_NORMAL,
            "warning_evaluated": False, "warning_fired": False,
            "warning_snapshot": None, "prev_snapshot": None,
            "up_volume_since_warning": 0.0, "down_volume_since_warning": 0.0,
            "interval_high": None, "interval_low": None,
            "prev_interval_high": None, "prev_interval_low": None,
            "persistent_failure_candidate_at_20m": False,
            "recovery_detected_at_15m": False, "recovery_confirmed_by_score": False,
            "stop_before_r": None, "stop_before_price": None,
            "requested_stop_r": None, "requested_stop_price": None,
            "stop_after_r": None, "stop_after_price": None,
            "stop_changed": False, "stop_change_ts": None,
            "checkpoint_log": [],
        }
    else:
        rpos["dynamic_risk_reduction"] = {
            **common,
            "scheduled_ts": entry_ts + pd.Timedelta(minutes=dynamic_cfg.get("trigger_offset_minutes", 10)),
            "checked": False, "actual_ts": None, "reason": None,
        }

    replay_bars = bars[bars.index >= entry_ts]
    for bar_ts, bar in replay_bars.iterrows():
        bt._update_excursion(rpos, bar)
        if not rpos["trail_activated"] and initial_risk and initial_risk > 0:
            price = float(bar["Close"])
            mfe_r_now = ((rpos["mfe_price"] - entry_price) if side == "long" else (entry_price - rpos["mfe_price"])) / initial_risk
            if mfe_r_now >= trailing_trigger_r:
                rpos["trail_activated"] = True
        if recovery_cfg is not None:
            bt._evaluate_v10_recovery(rpos, bars, bar_ts, side)
        else:
            bt._evaluate_v6_risk_event(rpos, bars, bar_ts, side)

    result = {"mechanism": mechanism, "applicable": True, "live_enforced": False}
    if recovery_cfg is not None:
        dr = rpos["dynamic_recovery"]
        result.update({
            "state": dr["state"], "warning_fired": dr["warning_fired"], "warning_evaluated": dr["warning_evaluated"],
            "recovery_confirmed_by_score": dr.get("recovery_confirmed_by_score", False),
            "recovery_detected_at_15m": dr.get("recovery_detected_at_15m", False),
            "stop_changed": dr.get("stop_changed", False),
            "replay_hard_stop_price": dr.get("stop_after_price"),
            "checkpoint_log": dr["checkpoint_log"],
        })
    else:
        drr = rpos["dynamic_risk_reduction"]
        result.update({
            "checked": drr["checked"], "reason": drr.get("reason"), "actual_ts": drr["actual_ts"].isoformat() if drr.get("actual_ts") is not None else None,
            "scheduled_ts": drr["scheduled_ts"].isoformat(),
            "current_r": drr.get("current_r"), "rsi_delta": drr.get("rsi_delta"),
            "returned_inside_or": drr.get("returned_inside_or"), "lost_ema9": drr.get("lost_ema9"),
            "mfe_r_so_far": drr.get("mfe_r_so_far"), "all_conditions_passed": drr.get("all_conditions_passed", False),
            "requested_adjusted_stop_price": drr.get("requested_adjusted_stop_price"),
            "stop_after_price": drr.get("stop_after_price"),
        })
    return result
