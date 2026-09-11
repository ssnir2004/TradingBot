"""Detector contract. Each of the four momentum setups is a subclass that
looks at one candidate's bars and returns a Signal or None.

All detectors operate on *closed* candles only (the loop strips the still-
forming last bar before building the context), at cfg["patterns"]
["entry_timeframe"] (5m by default). `price` is the near-real-time last
price from the TradingView scan, used for break/HOD proximity checks that
shouldn't wait for a 5-minute candle to close.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from momentum.scanner import Candidate
from momentum.signals import Signal


@dataclass
class DetectContext:
    cfg: dict
    candidate: Candidate
    price: float
    session_5m: pd.DataFrame      # today's CLOSED 5m bars, oldest first
    session_1m: pd.DataFrame | None
    daily: pd.DataFrame | None
    hod: float                    # session high of day (from closed bars)


class Detector:
    key: str = "?"
    name: str = "?"

    def evaluate(self, ctx: DetectContext) -> Signal | None:  # pragma: no cover
        raise NotImplementedError

    # -- shared helpers -------------------------------------------------
    def _strat_cfg(self, ctx: DetectContext) -> dict:
        return ctx.cfg[f"strategy_{self.key}"]

    def _mk_signal(self, ctx: DetectContext, entry_ref: float, *, conviction: str = "normal",
                   features: dict | None = None) -> Signal:
        return Signal(
            strategy=self.key,
            symbol=ctx.candidate.symbol,
            price=ctx.price,
            entry_ref=round(float(entry_ref), 4),
            timeframe=ctx.cfg["patterns"]["entry_timeframe"],
            conviction=conviction,
            above_preferred_range=ctx.candidate.above_preferred_range,
            features={
                "rvol": ctx.candidate.rvol,
                "change_from_open_pct": ctx.candidate.change_from_open_pct,
                "float_shares": ctx.candidate.float_shares,
                "hod": ctx.hod,
                **(features or {}),
            },
        )
