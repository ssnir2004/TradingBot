"""The HOD Momentum Scanner - a thin client over TradingView's public
screener endpoint (scanner.tradingview.com/america/scan). No login, no API
key: this is the same JSON endpoint tradingview.com's own screener page
calls. We stay a polite citizen - one request per poll_seconds (default
45s), no redistribution of the payload.

What it gives us for free (verified 2026-09-10): float_shares_outstanding,
relative_volume_10d_calc (intraday RVOL), change_from_open, volume,
average_volume_10d_calc, premarket_change. That is the entire G1-G6
screener except the per-minute volume spike (G2, computed from IBKR bars
in momentum.features) and the daily-chart overhead-resistance check (G6,
computed in momentum.features from yfinance daily bars).
"""
import logging
from dataclasses import dataclass, field

import requests

log = logging.getLogger("momentum.scanner")

SCAN_URL = "https://scanner.tradingview.com/america/scan"
TIMEOUT = 20

# order matters - it defines the tuple layout in each result's "d" array
COLUMNS = [
    "name", "exchange", "close", "change_from_open", "relative_volume_10d_calc",
    "float_shares_outstanding", "volume", "average_volume_10d_calc", "premarket_change",
]


@dataclass
class Candidate:
    symbol: str
    exchange: str
    price: float
    change_from_open_pct: float
    rvol: float
    float_shares: float
    volume: float
    avg_volume_10d: float
    premarket_pct: float | None
    passed_screener: bool = False
    reject_reason: str | None = None
    above_preferred_range: bool = False
    # filled in later by the loop
    extras: dict = field(default_factory=dict)

    def as_row(self) -> dict:
        return {
            "symbol": self.symbol, "exchange": self.exchange, "price": self.price,
            "change_from_open_pct": self.change_from_open_pct, "rvol": self.rvol,
            "float_shares": self.float_shares, "volume": self.volume,
            "avg_volume_10d": self.avg_volume_10d, "premarket_pct": self.premarket_pct,
            "passed_screener": int(self.passed_screener), "reject_reason": self.reject_reason,
        }


def _build_query(cfg: dict) -> dict:
    s = cfg["screener"]
    return {
        "filter": [
            {"left": "close", "operation": "in_range",
             "right": [s["price_hard_min"], s["price_hard_max"]]},
            {"left": "change_from_open", "operation": "greater",
             "right": s["min_change_from_open_pct"]},
            {"left": "float_shares_outstanding", "operation": "less",
             "right": s["float_max"]},
            {"left": "exchange", "operation": "in_range",
             "right": s["allowed_exchanges"]},
        ],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": COLUMNS,
        "sort": {"sortBy": "change_from_open", "sortOrder": "desc"},
        "range": [0, 100],
    }


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def fetch_raw(cfg: dict) -> list[Candidate]:
    """One HTTP call. Returns every row TradingView's own broad filter
    returned (price/gain/float/exchange), before our finer G1-G6 gates."""
    resp = requests.post(
        SCAN_URL, json=_build_query(cfg), timeout=TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
    )
    resp.raise_for_status()
    out = []
    for item in resp.json().get("data", []):
        d = dict(zip(COLUMNS, item.get("d", [])))
        out.append(Candidate(
            symbol=d.get("name", item.get("s", "?").split(":")[-1]),
            exchange=d.get("exchange", ""),
            price=_num(d.get("close")),
            change_from_open_pct=_num(d.get("change_from_open")),
            rvol=_num(d.get("relative_volume_10d_calc")),
            float_shares=_num(d.get("float_shares_outstanding")),
            volume=_num(d.get("volume")),
            avg_volume_10d=_num(d.get("average_volume_10d_calc")),
            premarket_pct=(None if d.get("premarket_change") in (None, "")
                           else _num(d.get("premarket_change"))),
        ))
    return out


def apply_screener(cands: list[Candidate], cfg: dict) -> list[Candidate]:
    """The G1/G3/G5 gates that go beyond TradingView's coarse filter. G2
    (volume spike) and G6 (overhead resistance) need bar data and are
    applied later, in the loop. Mutates each Candidate's passed_screener /
    reject_reason / above_preferred_range and returns the same list."""
    s = cfg["screener"]
    for c in cands:
        c.above_preferred_range = c.price > s["price_preferred_max"]
        reason = None
        if not (c.rvol >= s["rvol_min"]):
            reason = f"rvol {c.rvol:.1f} < {s['rvol_min']}"
        elif c.change_from_open_pct < s["min_change_from_open_pct"]:
            reason = f"change_from_open {c.change_from_open_pct:.1f}% < {s['min_change_from_open_pct']}%"
        elif not (c.float_shares < s["float_max"]):
            reason = f"float {c.float_shares:,.0f} >= {s['float_max']:,}"
        elif c.price < s["price_hard_min"] or c.price > s["price_hard_max"]:
            reason = f"price {c.price} outside hard [{s['price_hard_min']}, {s['price_hard_max']}]"
        c.passed_screener = reason is None
        c.reject_reason = reason
    return cands


def scan(cfg: dict) -> list[Candidate]:
    """fetch + apply_screener, resilient to a bad response (returns [])."""
    try:
        cands = fetch_raw(cfg)
    except (requests.RequestException, ValueError) as exc:
        log.warning("TradingView scan failed: %s", exc)
        return []
    return apply_screener(cands, cfg)
