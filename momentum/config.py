"""Every tunable in the momentum suite, with the defaults agreed during
planning (the "G1-G19" gap decisions - see docs/momentum_strategy_spec.md
for the rationale behind each number).

No hardcoded thresholds live anywhere else in the package: a detector or
the scanner always reads its numbers from the dict `load_config()` returns.
Overrides are stored as one JSON blob in the shared settings table under
the key `momentum:config_json` (deep-merged over DEFAULTS), so the future
dashboard screen can edit them without a code change and without a
schema migration.
"""
import copy
import json

from src import db

CONFIG_SETTING_KEY = "momentum:config_json"

DEFAULTS = {
    # ---- G1-G6 : screener / universe -----------------------------------
    "screener": {
        "poll_seconds": 45,                 # how often the scan loop hits TradingView
        "rvol_min": 5.0,                    # G1  relative_volume_10d_calc floor
        "volume_spike_mult": 2.0,           # G2  last-minute vol / trailing avg
        "volume_spike_lookback_min": 5,     # G2
        "price_preferred_min": 1.50,        # G3
        "price_preferred_max": 10.00,       # G3
        "price_hard_min": 1.00,             # G3  reject below (thin, non-marginable)
        "price_hard_max": 20.00,            # G3  reject above
        "min_change_from_open_pct": 10.0,   # "already up >=10% intraday"
        "float_max": 50_000_000,            # < 50M shares
        "entry_window_et": ["09:30", "12:00"],   # G4
        "premarket_enabled": False,         # G4
        "afternoon_enabled": False,         # G4
        "require_catalyst": False,          # G5  (not implemented in v1)
        "allowed_exchanges": ["NASDAQ", "NYSE", "AMEX"],  # drop OTC/pink (untradeable bars, wide spreads)
        "resistance_filter_mode": "annotate",   # G6  annotate | reject | off
        "resistance_lookback_days": 60,     # G6
        "resistance_headroom_pct": 10.0,    # G6  reject/flag if a prior daily high sits within this % above
        "max_shortlist": 25,               # safety cap on IBKR bar fetches per cycle
    },

    # ---- G7-G12 : shared exit / risk engine ---------------------------
    # (only the levels are *computed* in phase 1 - nothing is managed yet)
    "exit": {
        "stop_mode": "fixed_cents",         # G7  fixed_cents | pct | atr
        "stop_cents": 10,                   # G7
        "max_loss_cents": 20,               # G7
        "red_candle_exit_enabled": True,    # G8
        "red_candle_hold_min_r": 2.0,       # G8  hold through a red 5m candle only if >= this R
        "bailout_minutes": 5,               # G10 "breakout or bailout"
        "bailout_exit_type": "market",      # G10
        "first_scale_r": 1.0,               # G11 sell half here
        "breakeven_after_first_scale": True,  # G11
        "second_scale_r": 2.0,              # G11 sell half of the remainder
        "runner_trail": "prior_5m_low",     # G11
    },

    # ---- G9 : position sizing / circuit breakers ---------------------
    "risk": {
        "per_trade_pct": 0.5,               # % of account equity risked per trade
        "max_concurrent_positions": 1,
        "max_position_notional_usd": 3000,
        "daily_max_loss_usd": 200,
        "daily_max_trades": 5,
    },

    # ---- G13-G15 : shared pattern primitives -------------------------
    "patterns": {
        "entry_timeframe": "5m",            # G13
        "allow_1m": False,                  # G13
        "break_confirm_mode": "buffer",     # G14  buffer | close
        "break_buffer_cents": 3,            # G14
        "new_high_ref": "prior_candle",     # G15
        "swing_fractal_bars": 2,            # G15
        "signal_cooldown_min": 15,          # G15
        "max_signals_per_symbol_per_day": 3,  # G15
        "fresh_break_max_cents": 20,        # ignore a break we only see once price is already this far past the level
        "bars_lookback_min": 180,           # how much 5m history to pull per candidate
    },

    # ---- G16 : Strategy A - WholeHalfDollarBreak --------------------
    "strategy_A": {
        "enabled": True,
        "levels": ["whole", "half"],
        "arm_distance_cents": 15,
        "require_within_hod_pct": 2.0,
    },
    # ---- G17 : Strategy B - BullFlagFlatTop ------------------------
    "strategy_B": {
        "enabled": True,
        "impulse_min_candles": 3,
        "impulse_max_candles": 6,
        "impulse_max_counter_candles": 1,
        "pullback_min_candles": 2,
        "pullback_max_candles": 4,
        "flat_top_tol_pct": 0.5,
        "pullback_max_retrace_pct": 50.0,
    },
    # ---- G18 : Strategy C - MAPullback9ema ------------------------
    "strategy_C": {
        "enabled": True,
        "ma_period": 9,
        "ma_timeframe": "5m",
        "ma_price": "close",
        "sideways_min_candles": 4,
        "tap_tol_cents": 5,
        "conviction": "low",
    },
    # ---- G19 : Strategy D - Setup1234 -----------------------------
    "strategy_D": {
        "enabled": True,
        "swing_fractal_bars": 2,
        "require_higher_low": True,
        "max_base_candles": 15,
        "stop_mode": "shared",              # shared | pivot_low
    },

    # which strategies proceed past phase 1 (alert) toward live. Phase 1
    # runs every enabled detector regardless - this only gates phases 3+.
    "live_strategies": ["A"],
}


def _deep_merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config() -> dict:
    """DEFAULTS deep-merged with whatever JSON is saved under
    momentum:config_json (empty/absent -> plain DEFAULTS)."""
    raw = db.get_setting(CONFIG_SETTING_KEY, "")
    if not raw:
        return copy.deepcopy(DEFAULTS)
    try:
        override = json.loads(raw)
    except json.JSONDecodeError:
        return copy.deepcopy(DEFAULTS)
    return _deep_merge(DEFAULTS, override)


def save_override(override: dict) -> None:
    """Persist a (partial) override dict. Stored verbatim; merged at read."""
    db.set_setting(CONFIG_SETTING_KEY, json.dumps(override, indent=2, default=str))
