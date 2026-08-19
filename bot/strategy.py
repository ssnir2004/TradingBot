""""Trend Join Long" strategy: long-only, 5-minute chart, trend-following entries.

NOTE: This is a best-effort reconstruction of a long-only trend/momentum day
strategy in the spirit described by the source article (daily-trend filters +
intraday trend/volume filters on a 5-min chart), not a transcription of it —
the original article text was not reachable from this environment. Every
threshold below is a config-driven default; tune it to match your own
backtested rules in .env.

Filters applied, long-only:
  Daily (context):
    1. Price above the daily 50-SMA           -> primary uptrend filter
    2. Price above the daily 200-SMA           -> secondary/long-term uptrend filter
  Intraday (5-min bars):
    3. Price above VWAP                        -> intraday control by buyers
    4. 9-EMA above 20-EMA (and price above 9-EMA) -> short-term trend alignment
    5. Relative volume on the signal bar >= 1.5x the 20-bar average -> confirms interest
    6. Breakout above the high of the last N bars (opening-range / premarket high) -> trigger

Entry: on the close of a 5-min bar that satisfies all six filters.
Initial stop: low of the signal bar minus a small ATR buffer.
Take-profit (first partial target): entry + PARTIAL_PROFIT_R_MULTIPLE * (entry - stop).
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

import config

BREAKOUT_LOOKBACK_BARS = 6  # ~30 min opening range on a 5-min chart
ATR_PERIOD = 14
ATR_STOP_BUFFER_MULT = 0.25
MIN_SIGNAL_BAR_RVOL = 1.5


@dataclass
class Signal:
    symbol: str
    entry_price: float
    stop_price: float
    take_profit_price: float
    reason: str

    @property
    def risk_per_share(self) -> float:
        return self.entry_price - self.stop_price


def _vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    return (typical * df["Volume"]).cumsum() / df["Volume"].cumsum()


def _atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.rolling(period).mean()


def _passes_daily_trend(daily: pd.DataFrame) -> bool:
    if len(daily) < 200:
        return False
    close = daily["Close"]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    last_close = close.iloc[-1]
    return last_close > sma50 and last_close > sma200


def evaluate(symbol: str, daily_bars: pd.DataFrame, intraday_bars: pd.DataFrame) -> Signal | None:
    """Return a Signal if the symbol qualifies for entry on the latest closed bar."""
    if not _passes_daily_trend(daily_bars):
        return None

    df = intraday_bars.copy()
    if len(df) < max(BREAKOUT_LOOKBACK_BARS, 20) + 1:
        return None

    df["vwap"] = _vwap(df)
    df["ema9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["ema20"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["atr"] = _atr(df)
    df["avg_vol_20"] = df["Volume"].rolling(20).mean()

    signal_bar = df.iloc[-1]
    lookback = df.iloc[-(BREAKOUT_LOOKBACK_BARS + 1):-1]
    breakout_level = lookback["High"].max()

    above_vwap = signal_bar["Close"] > signal_bar["vwap"]
    ema_aligned = signal_bar["ema9"] > signal_bar["ema20"] and signal_bar["Close"] > signal_bar["ema9"]
    rvol_ok = (
        signal_bar["avg_vol_20"] > 0
        and signal_bar["Volume"] / signal_bar["avg_vol_20"] >= MIN_SIGNAL_BAR_RVOL
    )
    breakout = signal_bar["Close"] > breakout_level

    if not (above_vwap and ema_aligned and rvol_ok and breakout):
        return None

    entry_price = float(signal_bar["Close"])
    atr_buffer = float(signal_bar["atr"]) * ATR_STOP_BUFFER_MULT if not np.isnan(signal_bar["atr"]) else 0.0
    stop_price = float(signal_bar["Low"]) - atr_buffer
    risk = entry_price - stop_price
    if risk <= 0:
        return None

    take_profit_price = entry_price + config.PARTIAL_PROFIT_R_MULTIPLE * risk

    return Signal(
        symbol=symbol,
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
        reason=(
            f"daily uptrend + above VWAP + EMA9>EMA20 + RVOL="
            f"{signal_bar['Volume'] / signal_bar['avg_vol_20']:.1f}x + breakout>{breakout_level:.2f}"
        ),
    )
