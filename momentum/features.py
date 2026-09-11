"""Pure-function feature helpers shared by the four detectors and the
loop's G2/G6 gates. Everything here takes a normalised OHLCV DataFrame
(columns Open/High/Low/Close/Volume, tz-aware ET index, oldest first) and
returns plain numbers / small lists - no I/O, no config lookups.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# --------------------------------------------------------------- candles ---
def is_green(row) -> bool:
    return float(row["Close"]) > float(row["Open"])


def is_red(row) -> bool:
    return float(row["Close"]) < float(row["Open"])


def session_hod(session: pd.DataFrame) -> float:
    return float(session["High"].max())


def session_lod(session: pd.DataFrame) -> float:
    return float(session["Low"].min())


# --------------------------------------------------------------- EMA / MA ---
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def ema_last(session: pd.DataFrame, period: int, price: str = "close") -> float:
    col = {"close": "Close", "open": "Open", "high": "High", "low": "Low"}[price]
    return float(ema(session[col], period).iloc[-1])


# ------------------------------------------------------- swing detection ---
def swing_highs(df: pd.DataFrame, fractal: int = 2) -> list[int]:
    """Positional indices of bars whose High is strictly the local max over
    `fractal` bars on each side (a classic fractal pivot)."""
    highs = df["High"].to_numpy()
    out = []
    for i in range(fractal, len(highs) - fractal):
        window = highs[i - fractal:i + fractal + 1]
        if highs[i] == window.max() and (window.argmax() == fractal):
            out.append(i)
    return out


def swing_lows(df: pd.DataFrame, fractal: int = 2) -> list[int]:
    lows = df["Low"].to_numpy()
    out = []
    for i in range(fractal, len(lows) - fractal):
        window = lows[i - fractal:i + fractal + 1]
        if lows[i] == window.min() and (window.argmin() == fractal):
            out.append(i)
    return out


# ----------------------------------------------------------------- G2 ---
def volume_spike(minute_bars: pd.DataFrame, lookback: int, mult: float) -> tuple[bool, float]:
    """(spiked?, ratio) - last 1-minute bar's volume vs the mean of the
    `lookback` bars before it. Needs >= lookback+1 rows or returns
    (False, nan)."""
    if minute_bars is None or len(minute_bars) < lookback + 1:
        return False, float("nan")
    vols = minute_bars["Volume"].to_numpy(dtype=float)
    trailing = vols[-lookback - 1:-1]
    base = trailing.mean()
    if base <= 0:
        return False, float("nan")
    ratio = vols[-1] / base
    return ratio >= mult, ratio


# ----------------------------------------------------------------- G6 ---
def overhead_resistance(daily: pd.DataFrame, price: float, lookback_days: int,
                        headroom_pct: float) -> tuple[bool, float | None]:
    """(clear?, nearest_overhead_high). "clear" means no prior daily High
    sits within headroom_pct above `price` over the last `lookback_days`
    completed sessions. The still-forming last daily bar is excluded."""
    if daily is None or len(daily) < 3:
        return True, None
    window = daily.iloc[-(lookback_days + 1):-1]
    ceiling = price * (1 + headroom_pct / 100.0)
    overhead = window["High"][(window["High"] > price) & (window["High"] <= ceiling)]
    if overhead.empty:
        return True, None
    return False, float(overhead.min())


# --------------------------------------------------- level / break helpers ---
def round_levels_near(price: float, kinds: list[str], span: float) -> list[tuple[str, float]]:
    """Every whole- and half-dollar level within +/- `span` dollars of
    `price`, as (kind, level) pairs sorted ascending. Strategy A picks the
    one price has just crossed (was below, now at/above)."""
    out: list[tuple[str, float]] = []
    lo, hi = price - span, price + span
    if "whole" in kinds:
        lvl = float(np.floor(lo))
        while lvl <= hi:
            if lo <= lvl <= hi:
                out.append(("whole", round(lvl, 2)))
            lvl += 1.0
    if "half" in kinds:
        lvl = float(np.floor(lo * 2) / 2)
        while lvl <= hi:
            is_half = abs((lvl % 1.0) - 0.5) < 1e-6   # .50 only; whole handled above
            if lo <= lvl <= hi and is_half:
                out.append(("half", round(lvl, 2)))
            lvl += 0.5
    return sorted(out, key=lambda kv: kv[1])


def broke_level(price: float, level: float, mode: str, buffer_cents: float,
                last_closed_close: float | None = None) -> bool:
    """G14 break confirmation. 'buffer': price trades buffer_cents above the
    level. 'close': the last *closed* entry-timeframe candle closed above."""
    if mode == "close":
        return last_closed_close is not None and last_closed_close > level
    return price >= level + buffer_cents / 100.0


def new_high_vs_prior(session: pd.DataFrame) -> bool:
    """True if the last (closed) candle's High exceeds the prior candle's
    High - the G15 'first green candle that makes a new high' trigger."""
    if len(session) < 2:
        return False
    return float(session["High"].iloc[-1]) > float(session["High"].iloc[-2])


# ------------------------------------------------------ impulse / pullback ---
def split_impulse_pullback(session: pd.DataFrame, max_counter: int):
    """From the end of `session`: the pullback is the trailing contiguous
    run of non-green candles (the flag); the impulse is the green-dominated
    run immediately before it, extended left until it would take more than
    `max_counter` non-green candles. Returns (impulse_df, pullback_df);
    either may be empty.
    """
    n = len(session)
    if n == 0:
        return session.iloc[0:0], session.iloc[0:0]
    rows = [session.iloc[i] for i in range(n)]

    j = n - 1
    while j >= 0 and not is_green(rows[j]):
        j -= 1
    pullback = session.iloc[j + 1:n]

    i = j
    start = j + 1
    counter = 0
    while i >= 0:
        if is_green(rows[i]):
            start = i
        else:
            counter += 1
            if counter > max_counter:
                break
            start = i
        i -= 1
    impulse = session.iloc[start:j + 1]
    return impulse, pullback


def count_counter_candles(impulse: pd.DataFrame) -> int:
    return int(sum(1 for _, r in impulse.iterrows() if not is_green(r)))


def flat_top(pullback: pd.DataFrame, tol_pct: float) -> tuple[bool, float]:
    """Is the pullback a flat top - its candle highs within tol_pct of each
    other? Returns (flat?, the flat-top level = max high)."""
    if pullback is None or len(pullback) < 2:
        return False, float("nan")
    hs = pullback["High"].to_numpy(dtype=float)
    top = hs.max()
    spread_pct = (top - hs.min()) / top * 100.0
    return spread_pct <= tol_pct, float(top)


def retrace_pct(impulse: pd.DataFrame, pullback: pd.DataFrame) -> float:
    """How deep the pullback retraced the impulse, in % of the impulse
    range. >100 means it gave the whole move back."""
    if impulse is None or impulse.empty or pullback is None or pullback.empty:
        return float("nan")
    lo = float(impulse["Low"].min())
    hi = float(impulse["High"].max())
    rng = hi - lo
    if rng <= 0:
        return float("nan")
    return (hi - float(pullback["Low"].min())) / rng * 100.0
