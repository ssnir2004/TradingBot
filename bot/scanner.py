"""Premarket scanner: filters a watchlist down to symbols gapping up on
above-average volume, using free yfinance data (no paid data feed required).

This mirrors the "premarket scan" step of the strategy: price range, gap %,
and relative volume are the cheap, keyless filters available before the open.
"""
from __future__ import annotations

from dataclasses import dataclass

import yfinance as yf
from loguru import logger

import config


@dataclass
class ScanResult:
    symbol: str
    last_price: float
    prev_close: float
    gap_pct: float
    relative_volume: float
    avg_dollar_volume: float

    def passes_filters(self) -> bool:
        return (
            config.MIN_PRICE <= self.last_price <= config.MAX_PRICE
            and self.gap_pct >= config.MIN_GAP_PCT
            and self.relative_volume >= config.MIN_RELATIVE_VOLUME
            and self.avg_dollar_volume >= config.MIN_AVG_DOLLAR_VOLUME
        )


def _scan_symbol(symbol: str) -> ScanResult | None:
    try:
        ticker = yf.Ticker(symbol)
        daily = ticker.history(period="30d", interval="1d")
        if daily.empty or len(daily) < 2:
            return None

        prev_close = float(daily["Close"].iloc[-2])
        avg_volume_20d = float(daily["Volume"].iloc[-20:].mean())
        avg_dollar_volume = avg_volume_20d * prev_close

        intraday = ticker.history(period="1d", interval="1m", prepost=True)
        if intraday.empty:
            return None

        last_price = float(intraday["Close"].iloc[-1])
        premarket_volume = float(intraday["Volume"].sum())
        relative_volume = premarket_volume / avg_volume_20d if avg_volume_20d else 0.0
        gap_pct = ((last_price - prev_close) / prev_close) * 100 if prev_close else 0.0

        return ScanResult(
            symbol=symbol,
            last_price=last_price,
            prev_close=prev_close,
            gap_pct=gap_pct,
            relative_volume=relative_volume,
            avg_dollar_volume=avg_dollar_volume,
        )
    except Exception as exc:  # noqa: BLE001 - scanner must not crash the bot on bad data
        logger.warning("Scan failed for {}: {}", symbol, exc)
        return None


def run_premarket_scan(universe: list[str] | None = None) -> list[ScanResult]:
    universe = universe or config.SCAN_UNIVERSE
    logger.info("Running premarket scan over {} symbols", len(universe))

    results = []
    for symbol in universe:
        result = _scan_symbol(symbol)
        if result and result.passes_filters():
            results.append(result)

    results.sort(key=lambda r: r.relative_volume, reverse=True)
    logger.info("Premarket scan found {} candidates: {}", len(results), [r.symbol for r in results])
    return results
