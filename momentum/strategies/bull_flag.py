"""Strategy B - BullFlagFlatTop (G17).

Impulse : 3-6 mostly-green 5m candles (<=1 non-green inside) that put the
          stock on the HOD scanner.
Flag    : 2-4 non-green candles whose highs are flat (within flat_top_tol_pct)
          and that retrace no more than pullback_max_retrace_pct of the impulse.
Fire    : the last closed candle is green, makes a new high vs the prior
          candle, AND breaks the flat-top level per G14.
Entry ref = the flat-top level.
"""
from __future__ import annotations

from momentum import features as F
from momentum.signals import Signal
from momentum.strategies.base import DetectContext, Detector


class BullFlagFlatTop(Detector):
    key = "B"
    name = "BullFlagFlatTop"

    def evaluate(self, ctx: DetectContext) -> Signal | None:
        sc = self._strat_cfg(ctx)
        p = ctx.cfg["patterns"]
        s = ctx.session_5m
        if len(s) < sc["impulse_min_candles"] + sc["pullback_min_candles"] + 1:
            return None

        last = s.iloc[-1]
        if not F.is_green(last):
            return None

        # split everything up to (but not including) the firing candle
        prior = s.iloc[:-1]
        impulse, pullback = F.split_impulse_pullback(prior, sc["impulse_max_counter_candles"])

        if not (sc["impulse_min_candles"] <= len(impulse) <= sc["impulse_max_candles"]):
            return None
        if not (sc["pullback_min_candles"] <= len(pullback) <= sc["pullback_max_candles"]):
            return None
        if F.count_counter_candles(impulse) > sc["impulse_max_counter_candles"]:
            return None

        flat, top = F.flat_top(pullback, sc["flat_top_tol_pct"])
        if not flat:
            return None

        rp = F.retrace_pct(impulse, pullback)
        if rp != rp or rp > sc["pullback_max_retrace_pct"]:   # nan or too deep
            return None

        # impulse should be a real move up
        if float(impulse["Close"].iloc[-1]) <= float(impulse["Open"].iloc[0]):
            return None

        # G15 new high + G14 break of the flat top
        if not (float(last["High"]) > float(s.iloc[-2]["High"])):
            return None
        if not F.broke_level(ctx.price, top, p["break_confirm_mode"],
                             p["break_buffer_cents"], float(last["Close"])):
            return None

        return self._mk_signal(
            ctx, entry_ref=top,
            features={
                "impulse_candles": len(impulse),
                "pullback_candles": len(pullback),
                "flat_top": round(top, 4),
                "retrace_pct": round(rp, 1),
                "impulse_low": round(float(impulse["Low"].min()), 4),
            },
        )
