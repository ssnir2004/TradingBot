"""US/Eastern market-hours helpers (no external market-calendar dependency)."""
from datetime import datetime, time
from zoneinfo import ZoneInfo

import config

EASTERN = ZoneInfo(config.TIMEZONE)


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))

PREMARKET_SCAN_T = _parse_hhmm(config.PREMARKET_SCAN_TIME)
MARKET_OPEN_T = _parse_hhmm(config.MARKET_OPEN_TIME)
MARKET_CLOSE_T = _parse_hhmm(config.MARKET_CLOSE_TIME)
FORCE_CLOSE_T = _parse_hhmm(config.FORCE_CLOSE_TIME)


def now_eastern() -> datetime:
    return datetime.now(EASTERN)


def is_weekday(dt: datetime) -> bool:
    return dt.weekday() < 5


def is_market_open(dt: datetime | None = None) -> bool:
    dt = dt or now_eastern()
    return is_weekday(dt) and MARKET_OPEN_T <= dt.time() < MARKET_CLOSE_T


def is_past_force_close(dt: datetime | None = None) -> bool:
    dt = dt or now_eastern()
    return dt.time() >= FORCE_CLOSE_T


def is_premarket_scan_time(dt: datetime | None = None) -> bool:
    dt = dt or now_eastern()
    return is_weekday(dt) and dt.time() >= PREMARKET_SCAN_T and dt.time() < MARKET_OPEN_T
