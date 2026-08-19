"""Shared paper/live mode helpers — kept dependency-light (no pandas/yfinance)
so trade.py and bot.py can import it without pulling in cycle.py's heavier
imports. `db` itself only needs sqlite3/bcrypt, so importing it here doesn't
add any heavy dependency.
"""
from src import db

# Gateway ports (paper/live are separate IB Gateway processes on this box —
# see DEPLOY.md). Client ids are shared across modes since each mode talks
# to a different Gateway process, so there's no collision between them.
GATEWAY_PORT_BY_MODE = {"paper": "4002", "live": "4001"}

# key -> (.env suffix, cast, hardcoded fallback if neither DB nor .env has it)
RISK_PARAM_SPECS = {
    "portfolio_value": ("PORTFOLIO_VALUE_USD", float, 25000),
    "max_trades_per_day": ("MAX_TRADES_PER_DAY", int, 5),
    "max_risk_pct": ("MAX_RISK_PER_TRADE_PCT", float, 1.0),
}


def ibkr_port(env: dict, mode: str) -> int:
    key = f"{mode.upper()}_IBKR_PORT"
    return int(env.get(key, GATEWAY_PORT_BY_MODE[mode]))


def risk_params(env: dict, mode: str) -> dict:
    """Per-param precedence: a value saved from the dashboard (DB) wins;
    otherwise falls back to .env; otherwise the hardcoded default. This way
    a fresh deploy behaves exactly as before (.env-driven) until someone
    actually changes a value from the dashboard's Settings screen."""
    prefix = mode.upper()
    result = {}
    for key, (env_suffix, cast, default) in RISK_PARAM_SPECS.items():
        override = db.get_setting(f"{mode}:risk:{key}", "")
        if override != "":
            result[key] = cast(override)
        else:
            result[key] = cast(env.get(f"{prefix}_{env_suffix}", default))
    return result
