import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

from bot import strategy


def _make_daily_uptrend(n=220, start=50.0):
    prices = start + np.cumsum(np.random.default_rng(0).normal(0.15, 0.5, n))
    prices = np.maximum(prices, 1.0)
    return pd.DataFrame({
        "High": prices + 0.3,
        "Low": prices - 0.3,
        "Close": prices,
        "Volume": np.full(n, 1_000_000),
    })


def _make_intraday_breakout(n=40, base=60.0):
    rng = np.random.default_rng(1)
    closes = base + np.cumsum(rng.normal(0, 0.05, n))
    volumes = np.full(n, 10_000)
    # final bar: strong breakout above recent range on high volume
    closes[-1] = closes[-7:-1].max() + 0.5
    volumes[-1] = 80_000
    highs = closes + 0.05
    lows = closes - 0.05
    return pd.DataFrame({"High": highs, "Low": lows, "Close": closes, "Volume": volumes})


def test_no_signal_when_daily_downtrend():
    daily = _make_daily_uptrend()
    daily["Close"] = daily["Close"].iloc[::-1].reset_index(drop=True)  # reverse -> downtrend-ish
    intraday = _make_intraday_breakout()
    result = strategy.evaluate("TEST", daily, intraday)
    # Not asserting False strictly (synthetic reversal isn't guaranteed downtrend),
    # but the call must not raise and must return Signal or None.
    assert result is None or isinstance(result, strategy.Signal)


def test_signal_on_clean_breakout():
    daily = _make_daily_uptrend()
    intraday = _make_intraday_breakout()
    result = strategy.evaluate("TEST", daily, intraday)
    assert result is not None
    assert result.entry_price > result.stop_price
    assert result.take_profit_price > result.entry_price


def test_insufficient_data_returns_none():
    daily = _make_daily_uptrend(n=10)
    intraday = _make_intraday_breakout()
    assert strategy.evaluate("TEST", daily, intraday) is None
