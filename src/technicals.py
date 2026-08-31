"""Standard-industry technical indicator series - the ones orb.py/cycle.py
don't already have their own copy of (see their own _compute_rsi_series/
_compute_ema_series/_compute_vwap_series, reused directly by
src/telemetry_engine.py rather than duplicated here).

Every function here is pure pandas over an already-loaded bars DataFrame or
Series - no IBKR/DB dependency, no strategy-decision role of any kind (see
telemetry_engine.py's own module docstring: this whole feature is a passive
observation system, and these are its only computation primitives). Deliberately
plain, well-known formulas (Wilder's ADX/ATR, standard MACD/Stochastic/OBV) -
"use standard industry indicators only" per the Trade Telemetry Dashboard spec,
not a bespoke variant tuned for any one strategy.
"""
import pandas as pd


def macd_series(closes: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Standard MACD: EMA(fast) - EMA(slow), its own EMA(signal) as the
    signal line, and their difference as the histogram. Columns: macd,
    signal, histogram - all aligned to closes.index."""
    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "histogram": macd_line - signal_line})


def stochastic_series(high: pd.Series, low: pd.Series, close: pd.Series, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
    """Standard (slow) Stochastic Oscillator: %K = 100 * (close - lowest
    low over k_period) / (highest high over k_period - lowest low over
    k_period); %D = simple moving average of %K over d_period. Columns:
    percent_k, percent_d - NaN wherever fewer than k_period/d_period bars
    have accumulated yet, same "no fabricated value before enough history"
    convention every other series function in this codebase already uses."""
    lowest_low = low.rolling(k_period, min_periods=k_period).min()
    highest_high = high.rolling(k_period, min_periods=k_period).max()
    denom = (highest_high - lowest_low).replace(0, pd.NA)
    percent_k = 100 * (close - lowest_low) / denom
    percent_d = percent_k.rolling(d_period, min_periods=d_period).mean()
    return pd.DataFrame({"percent_k": percent_k, "percent_d": percent_d})


def atr_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's ATR at every bar - true range (max of high-low, |high -
    prior close|, |low - prior close|) smoothed with Wilder's own
    alpha=1/period EWM, same smoothing convention orb._compute_rsi_series
    already uses for RSI. Deliberately a separate function from cycle.
    _compute_atr, which is DAILY-bar-based and only returns the latest
    value - this one is intraday-bar-based and returns the full series,
    needed for ATR Expansion (current / entry-time ATR) at each snapshot."""
    prev_close = close.shift(1)
    true_range = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def adx_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.DataFrame:
    """Wilder's ADX/+DI/-DI. Directional movement is the larger of the
    up-move/down-move at each bar (zeroed out when negative or when the
    other direction's move is larger - Wilder's own tie-breaking rule),
    smoothed the same Wilder alpha=1/period way as true range itself.
    Columns: adx, plus_di, minus_di."""
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    prev_close = close.shift(1)
    true_range = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    smoothed_tr = true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / smoothed_tr.replace(0, pd.NA)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / smoothed_tr.replace(0, pd.NA)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)
    adx = dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return pd.DataFrame({"adx": adx, "plus_di": plus_di, "minus_di": minus_di})


def obv_series(close: pd.Series, volume: pd.Series) -> pd.Series:
    """Standard On Balance Volume: running total of +volume on an up
    close, -volume on a down close, 0 on an unchanged close. Continuous
    across the whole `close`/`volume` series (not reset per day) - same
    "multi-day running series" convention as RSI/EMA/MACD/ADX above,
    unlike VWAP which is deliberately session-reset."""
    direction = close.diff().apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))
    return (direction * volume).fillna(0.0).cumsum()
