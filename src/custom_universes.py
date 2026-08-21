"""Registry of named custom stock universes beyond the default S&P 500 scan.

A strategy restricts itself to one of these via its rules_json's
universe_filters.custom_universe field (see cycle._strategy_universe); the
actual ticker list + fundamentals screen (market cap, beta, analyst rating)
is computed by build_custom_universe.py, which needs live market data and
so is meant to run on the deployed server (or any machine with real
internet access), not in a sandboxed dev session - this module only knows
where the result gets cached and how stale is too stale to trust.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_DIR / "data" / "universes"
ET = ZoneInfo("America/New_York")

CUSTOM_UNIVERSES = {
    "ixic_large_beta_buy": {
        "label": "NASDAQ Composite — market cap > $1B, beta > 1.2, analyst rating Buy+",
        "cache_path": CACHE_DIR / "ixic_large_beta_buy.json",
        # Analyst ratings and beta drift over weeks, not months (unlike the
        # S&P 500's own constituent list, which the project already treats
        # as fine to regenerate every 2-3 months) - a cache older than this
        # is treated as if it doesn't exist, so a strategy pinned to this
        # universe just quietly stops finding candidates rather than
        # trading against a stale screen. Re-run build_custom_universe.py
        # (ideally on a weekly schedule) well before this window closes.
        "max_staleness_days": 14,
    },
}


def load_custom_universe(key: str) -> list[str]:
    """Returns the cached ticker list for this universe, or [] if it's
    never been generated, is corrupt, or is older than max_staleness_days."""
    spec = CUSTOM_UNIVERSES.get(key)
    if not spec or not spec["cache_path"].exists():
        return []
    try:
        payload = json.loads(spec["cache_path"].read_text())
        generated_at = datetime.fromisoformat(payload["generated_at"])
        tickers = payload["tickers"]
    except (json.JSONDecodeError, OSError, KeyError, ValueError):
        return []
    if datetime.now(ET) - generated_at > timedelta(days=spec["max_staleness_days"]):
        return []
    return tickers


def load_all_custom_universes() -> dict[str, list[str]]:
    """Only includes universes with a valid, non-stale cache."""
    result = {}
    for key in CUSTOM_UNIVERSES:
        tickers = load_custom_universe(key)
        if tickers:
            result[key] = tickers
    return result
