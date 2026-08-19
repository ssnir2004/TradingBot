"""Shared paper/live mode helpers — kept dependency-light (no pandas/yfinance)
so trade.py and bot.py can import it without pulling in cycle.py's heavier
imports.
"""

# Gateway ports (paper/live are separate IB Gateway processes on this box —
# see DEPLOY.md). Client ids are shared across modes since each mode talks
# to a different Gateway process, so there's no collision between them.
GATEWAY_PORT_BY_MODE = {"paper": "4002", "live": "4001"}


def ibkr_port(env: dict, mode: str) -> int:
    key = f"{mode.upper()}_IBKR_PORT"
    return int(env.get(key, GATEWAY_PORT_BY_MODE[mode]))


def risk_params(env: dict, mode: str) -> dict:
    prefix = mode.upper()
    return {
        "portfolio_value": float(env.get(f"{prefix}_PORTFOLIO_VALUE_USD", 25000)),
        "max_trade_size": float(env.get(f"{prefix}_MAX_TRADE_SIZE_USD", 2500)),
        "max_trades_per_day": int(env.get(f"{prefix}_MAX_TRADES_PER_DAY", 5)),
        "max_risk_pct": float(env.get(f"{prefix}_MAX_RISK_PER_TRADE_PCT", 1.0)),
    }
