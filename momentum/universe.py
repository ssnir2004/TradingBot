"""Broad-universe helpers for momentum.backfill - reuses the repo's
existing NASDAQ symbol-directory fetch (build_custom_universe.py) rather
than re-scraping it, and caches float lookups to disk since float is
essentially static (unlike price/volume) and a per-symbol yfinance .info
call is the slow part of a backfill run.
"""
import json
import logging
import time
from pathlib import Path

import yfinance as yf

from build_custom_universe import fetch_nasdaq_listed_symbols  # noqa: F401 (re-exported)

log = logging.getLogger("momentum.universe")

PROJECT_DIR = Path(__file__).resolve().parent.parent
FLOAT_CACHE_PATH = PROJECT_DIR / "data" / "momentum_bars" / "_float_cache.json"
FLOAT_CACHE_MAX_AGE_DAYS = 30


def _load_float_cache() -> dict:
    if not FLOAT_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(FLOAT_CACHE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_float_cache(cache: dict) -> None:
    FLOAT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FLOAT_CACHE_PATH.write_text(json.dumps(cache))


def float_shares_bulk(symbols: list[str]) -> dict[str, float | None]:
    """{symbol: floatShares or None}. Reuses a disk cache keyed by symbol
    (no expiry check beyond FLOAT_CACHE_MAX_AGE_DAYS - float drifts on the
    order of months via secondary offerings/buybacks, not days) so a
    re-run of the backfill over the same symbols is near-instant."""
    cache = _load_float_cache()
    now = time.time()
    max_age = FLOAT_CACHE_MAX_AGE_DAYS * 86400
    out: dict[str, float | None] = {}
    to_fetch = []
    for s in symbols:
        entry = cache.get(s)
        if entry is not None and (now - entry.get("t", 0)) < max_age:
            out[s] = entry.get("float")
        else:
            to_fetch.append(s)

    for i, s in enumerate(to_fetch):
        val = None
        for attempt in range(2):
            try:
                info = yf.Ticker(s).get_info()
                val = info.get("floatShares")
                break
            except Exception:
                if attempt == 0:
                    time.sleep(1)
        out[s] = val
        cache[s] = {"float": val, "t": now}
        if i % 25 == 0:
            log.info("float lookup %s/%s", i, len(to_fetch))
            _save_float_cache(cache)
    _save_float_cache(cache)
    return out
